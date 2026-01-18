#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>


#define BLOCK_SIZE 4096
#define FS_MAGIC 0x56534653      // "VSFS"
#define JOURNAL_MAGIC 0x4A524E4C // "JRNL"
#define NAME_LEN 28
#define JOURNAL_BLOCKS 16


// Strictly 128 bytes 
struct superblock {
    uint32_t magic;
    uint32_t block_size;
    uint32_t total_blocks;
    uint32_t inode_count;
    uint32_t journal_block;
    uint32_t inode_bitmap;
    uint32_t data_bitmap;
    uint32_t inode_start;
    uint32_t data_start;
    uint8_t _pad[128 - 9*4]; 
} __attribute__((packed));

struct inode {
    uint16_t type;  // 0=free, 1=file, 2=dir
    uint16_t links;
    uint32_t size;
    uint32_t direct[8];
    uint32_t ctime;
    uint32_t mtime;
    uint8_t _pad[128 - (2+2+4 + 8*4+4+4)]; 
} __attribute__((packed));

struct dirent {
    uint32_t inode;
    char name[NAME_LEN];
} __attribute__((packed));

struct journal_header {
    uint32_t magic;
    uint32_t nbytes_used;
} __attribute__((packed));


enum RecordType { REC_DATA = 1, REC_COMMIT = 2 };

struct rec_header {
    uint16_t type;
    uint16_t size;
} __attribute__((packed));

struct data_record {
    struct rec_header hdr;
    uint32_t block_no;
    uint8_t data[BLOCK_SIZE];
} __attribute__((packed));

struct commit_record {
    struct rec_header hdr;
} __attribute__((packed));



int fd;
struct superblock sb;


void read_block(uint32_t block_num, void *buf) {
    off_t offset = block_num * BLOCK_SIZE;
    if (lseek(fd, offset, SEEK_SET) == -1) {
        perror("lseek failed");
        exit(1);
    }

    size_t total_read = 0;
    char *ptr = (char *)buf; 
    while (total_read < BLOCK_SIZE) {
        ssize_t bytes = read(fd, ptr + total_read, BLOCK_SIZE - total_read);
        if (bytes < 0) { 
            perror("Read failed");
            exit(1);
        }
        if (bytes == 0) { 
            fprintf(stderr, "Error: Unexpected EOF at block %d\n", block_num);
            exit(1);
        }
        total_read += bytes;
    }
}


void write_block(uint32_t block_num, void *buf) {
    off_t offset = block_num * BLOCK_SIZE;
    if (lseek(fd, offset, SEEK_SET) == -1) {
        perror("lseek failed");
        exit(1);
    }
    if (write(fd, buf, BLOCK_SIZE) != BLOCK_SIZE) {
        perror("Write failed");
        exit(1);
    }
}




void do_install() {
    uint8_t *jbuf = NULL;
    size_t jsize = JOURNAL_BLOCKS * BLOCK_SIZE;

    jbuf = malloc(jsize);
    if (!jbuf) {
        perror("malloc");
        exit(1);
    }

    
    for (int i = 0; i < JOURNAL_BLOCKS; i++) {
        read_block(sb.journal_block + i, jbuf + i * BLOCK_SIZE);
    }

    struct journal_header *jh = (struct journal_header *)jbuf;
    if (jh->magic != JOURNAL_MAGIC) {
       
        free(jbuf);
        return;
    }

    if (jh->nbytes_used <= sizeof(struct journal_header)) {
       
        free(jbuf);
        return;
    }

    size_t pos = sizeof(struct journal_header);

    
    struct {
        uint32_t block_no;
        uint8_t *data;
    } *pending = NULL;
    size_t pending_cap = 0, pending_cnt = 0;

    // Scan the journal
    while (pos + sizeof(struct rec_header) <= jh->nbytes_used) {
        struct rec_header *rh = (struct rec_header *)(jbuf + pos);

        if (pos + rh->size > jh->nbytes_used) break; 

        if (rh->type == REC_DATA) {
            // Validate size
            size_t expected = sizeof(struct rec_header) + sizeof(uint32_t) + BLOCK_SIZE;
            if (rh->size != expected) {
                pos += rh->size;
                continue;
            }

            uint8_t *p = jbuf + pos + sizeof(struct rec_header);
            uint32_t block_no = *(uint32_t *)p;
            uint8_t *data = p + sizeof(uint32_t);

            // Add to pending list
            if (pending_cnt == pending_cap) {
                size_t newcap = pending_cap ? pending_cap * 2 : 16;
                pending = realloc(pending, newcap * sizeof(*pending));
                if (!pending) { perror("realloc"); free(jbuf); exit(1); }
                pending_cap = newcap;
            }
            pending[pending_cnt].block_no = block_no;
            pending[pending_cnt].data = data;
            pending_cnt++;
        } 
        else if (rh->type == REC_COMMIT) {
          
            for (size_t i = 0; i < pending_cnt; i++) {
                uint32_t bno = pending[i].block_no;
                if (bno < sb.total_blocks) {
                    write_block(bno, pending[i].data);
                }
            }
            pending_cnt = 0; // Reset for next transaction
        } 

        pos += rh->size;
    }

   
    memset(jbuf, 0, jsize);
    jh = (struct journal_header *)jbuf;
    jh->magic = JOURNAL_MAGIC;
    jh->nbytes_used = sizeof(struct journal_header);

    // Write cleared journal blocks back to disk
    for (int i = 0; i < JOURNAL_BLOCKS; i++) {
        write_block(sb.journal_block + i, jbuf + i * BLOCK_SIZE);
    }

    if (pending) free(pending);
    free(jbuf);
}




void do_create(char *filename) {
    uint8_t ibmap[BLOCK_SIZE];
    uint8_t dblock[BLOCK_SIZE];
    uint8_t new_inode_block[BLOCK_SIZE]; 
    uint8_t root_inode_block[BLOCK_SIZE]; // Might be needed separately

    // 1. Read Inode Bitmap
    read_block(sb.inode_bitmap, ibmap);
    
    // Find free inode
    int chosen_inode = -1;
    for (uint32_t idx = 0; idx < sb.inode_count; idx++) {
        uint8_t mask = 1 << (idx % 8);
        if ((ibmap[idx / 8] & mask) == 0) {
            ibmap[idx / 8] |= mask; 
            chosen_inode = idx;
            break;
        }
    }

    if (chosen_inode == -1) {
        printf("Error: No free inodes\n");
        return;
    }

 
    uint32_t inodes_per_block = BLOCK_SIZE / sizeof(struct inode);
    uint32_t new_inode_blk_offset = chosen_inode / inodes_per_block;
    uint32_t new_inode_real_block = sb.inode_start + new_inode_blk_offset;
    
    read_block(new_inode_real_block, new_inode_block);
    
    struct inode *inodes_arr = (struct inode *)new_inode_block;
    int inode_idx_in_block = chosen_inode % inodes_per_block;
    
    memset(&inodes_arr[inode_idx_in_block], 0, sizeof(struct inode));
    inodes_arr[inode_idx_in_block].type = 1;  // File 
    inodes_arr[inode_idx_in_block].links = 1;
    inodes_arr[inode_idx_in_block].size = 0;


    int root_inode_needs_log = 0;

    if (new_inode_real_block == sb.inode_start) {
      
        inodes_arr[0].size += sizeof(struct dirent);
    } else {
      
        read_block(sb.inode_start, root_inode_block);
        struct inode *root_ptr = (struct inode *)root_inode_block;
        root_ptr[0].size += sizeof(struct dirent);
        root_inode_needs_log = 1;
    }


   
    uint8_t temp_root[BLOCK_SIZE];
    read_block(sb.inode_start, temp_root);
    struct inode *root_node = (struct inode *)temp_root;
    uint32_t root_dir_data_block = root_node->direct[0]; 

    read_block(root_dir_data_block, dblock);
    struct dirent *d = (struct dirent *)dblock;
    
    int dir_index = -1;
    for (size_t i = 0; i < BLOCK_SIZE / sizeof(struct dirent); i++) {
        if (d[i].inode == 0 && d[i].name[0] == '\0') {
            dir_index = i;
            break;
        }
    }

    if (dir_index < 0) {
        printf("Error: Directory full\n");
        return;
    }

    d[dir_index].inode = chosen_inode;
    strncpy(d[dir_index].name, filename, NAME_LEN);


    // 5. Journaling
    struct journal_header jh;
    lseek(fd, sb.journal_block * BLOCK_SIZE, SEEK_SET);
    if (read(fd, &jh, sizeof(struct journal_header)) != sizeof(struct journal_header)) {
        return;
    }

    if (jh.magic != JOURNAL_MAGIC) {
        jh.magic = JOURNAL_MAGIC;
        jh.nbytes_used = sizeof(struct journal_header);
    }

   
    int blocks_to_log = 3 + root_inode_needs_log;
    size_t transaction_size = blocks_to_log * sizeof(struct data_record) + sizeof(struct commit_record);
    
    if (jh.nbytes_used + transaction_size > JOURNAL_BLOCKS * BLOCK_SIZE) {
        printf("Journal full. Please run install.\n");
        return;
    }

    off_t write_pos = (sb.journal_block * BLOCK_SIZE) + jh.nbytes_used;

    
    #define WRITE_RECORD(target_blk, src_buf) { \
        struct data_record r; \
        r.hdr.type = REC_DATA; \
        r.hdr.size = sizeof(struct data_record); \
        r.block_no = (target_blk); \
        memcpy(r.data, (src_buf), BLOCK_SIZE); \
        lseek(fd, write_pos, SEEK_SET); \
        write(fd, &r, sizeof(struct data_record)); \
        write_pos += sizeof(struct data_record); \
    }

    // Log the blocks
    WRITE_RECORD(sb.inode_bitmap, ibmap);
    WRITE_RECORD(new_inode_real_block, new_inode_block);
    
    if (root_inode_needs_log) {
        WRITE_RECORD(sb.inode_start, root_inode_block);
    }

    WRITE_RECORD(root_dir_data_block, dblock);

    struct commit_record c;
    c.hdr.type = REC_COMMIT;
    c.hdr.size = sizeof(struct commit_record);
    
    lseek(fd, write_pos, SEEK_SET);
    write(fd, &c, sizeof(struct commit_record));
    write_pos += sizeof(struct commit_record);

    // Update Header
    jh.nbytes_used = write_pos - (sb.journal_block * BLOCK_SIZE);
    lseek(fd, sb.journal_block * BLOCK_SIZE, SEEK_SET);
    write(fd, &jh, sizeof(struct journal_header));

    fsync(fd);
}




int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <command> [args]\n", argv[0]);
        return 1;
    }

    fd = open("vsfs.img", O_RDWR);
    if (fd < 0) {
        perror("vsfs.img not found");
        return 1;
    }

    uint8_t temp_sb_buf[BLOCK_SIZE];
    read_block(0, temp_sb_buf);
    memcpy(&sb, temp_sb_buf, sizeof(struct superblock));

    if (sb.magic != FS_MAGIC) {
        fprintf(stderr, "Invalid VSFS image\n");
        close(fd);
        return 1;
    }

    if (strcmp(argv[1], "install") == 0) {
        do_install();
    } else if (strcmp(argv[1], "create") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s create <filename>\n", argv[0]);
            return 1;
        }
        do_create(argv[2]);
    } else {
        fprintf(stderr, "Unknown command: %s\n", argv[1]);
    }

    close(fd);
    return 0;
}