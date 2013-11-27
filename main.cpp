#include <iostream>
#include "filereader.h"
#include "str.h"

#define TBL_SIZE 1001

using std::cout;
using std::cin;
using std::endl;

void debug(string message) {
	//cout<<"[dbg] "<<message<<endl;
}

class Hash{
	
	typedef struct {	// Financial record structure
		int    num;		// Transaction number
		float  amt;		// Amount of the transaction
		char   type;	// Transaction type (D=debit, C=credit)
		int    acct;	// Account used for transaction
		int    hour;	// Hour of transaction (0-23)
		int    min;		// Minute of transaction (0-59)
		int    day;		// Day of transaction (0-364)
		int    year;	// Year of transaction (0000-9999)
	} f_rec;

	typedef struct {	// Chained hash table node
		int   k;	    // Key of record node represents
		long  rec;	    // Offset of corresponding record in database file
		long  next;	    // Offset of next node in this chain (-1 for none)
	} ch_node;
	
	filereader idx;
	filereader db;
	
	public:
		Hash(string, string);
		void add(string);
		void find(int);
		void del(int);
		void print();
		void end();
		
};

Hash::Hash(string index_file, string db_file) {
	
	remove (index_file);
	remove (db_file);
	
	int i;
	ch_node *node = new ch_node;
	
	idx.open(index_file, 'x', 1, 1);
	
	//  If file empty, append 1001 nodes with a key = -1	
	idx.seek( 0, END );
	if ( idx.offset() == 0 ) {
		node->k = -1;
		for( i = 0; i < TBL_SIZE; i++ ) {
			idx.write_raw( (char *) node, sizeof( ch_node ) );
		}
	}
	
	db.open(db_file, 'x', 1, 1);
	
}

void Hash::add(string line) {
	string part[9];
	line.token(part,9);
	
	f_rec *record = new f_rec;
	record->num = (int) part[1];
	record->amt = (float) part[2];
	record->type = part[3][0];
	record->acct = (int) part[4];
	record->hour = (int) part[5];
	record->min = (int) part[6];
	record->day = (int) part[7];
	record->year = (int) part[8];
	
	long rec_off_in_db;
	long rec_off_in_index;
	long temp_offset;
	int hash_value;
	
	//prepare writing the record in the db file
	db.seek(0, END);
	rec_off_in_db = db.offset();
	
	//creating the entry to index file
	ch_node *entry = new ch_node;
	entry->k = record->num;
	entry->rec = rec_off_in_db;
	entry->next = -1;
	
	hash_value = entry->k % TBL_SIZE;
	
	//cout<<record->num<<endl;
	
	//read the node at the hash value in index file
	ch_node *temp = new ch_node;
	idx.seek(hash_value * sizeof(ch_node), BEGIN);
	temp_offset = idx.offset();
	idx.read_raw( (char *) temp, sizeof(ch_node) );
	
	if(temp->k == -1) {
		//simply write the record at the hash value
		idx.seek(hash_value * sizeof(ch_node), BEGIN);
		idx.write_raw( (char *) entry, sizeof( ch_node ) );
	}
	
	else {
		//COLLISION
		if(temp->k == entry->k) {
			cout<<"Record "<<temp->k<<" is a duplicate."<<endl;
			return;
		}
		
		//append it to the list at the hash vlaue
		while(temp->next != -1) {
			temp_offset = temp->next;
			idx.seek(temp_offset, BEGIN);
			idx.read_raw( (char *) temp, sizeof( ch_node ) );
			if(temp->k == entry->k) {
				cout<<"Record "<<temp->k<<" is a duplicate."<<endl;
				return;
			}
		}
		
		//append to the end of the list
		idx.seek(0, END);
		rec_off_in_index = idx.offset();
		idx.write_raw( (char *) entry, sizeof( ch_node ) );
		
		temp->next = rec_off_in_index;
		idx.seek(temp_offset, BEGIN);
		idx.write_raw( (char *) temp, sizeof( ch_node ) );
	}
	
	//actually writing the record in the db file
	db.seek(0, END);
	db.write_raw( (char *) record, sizeof( f_rec ) );
}

void Hash::find(int key) {
	f_rec *record = new f_rec;
	ch_node *temp = new ch_node;
	long temp_offset;
	int hash_value;
	hash_value = key % TBL_SIZE;
	idx.seek(hash_value * sizeof(ch_node), BEGIN);
	idx.read_raw( (char *) temp, sizeof( ch_node ) );
	if(temp->k == -1) {
		cout<<"Record "<<key<<" does not exist."<<endl;
		return;
	}
	else {
		while(temp->next != -1) {
			if(temp->k == key) {
				db.seek(temp->rec, BEGIN);
				db.read_raw( (char *) record, sizeof( f_rec ) );
				cout<<record->num<<" "<<record->amt<<" "<<record->type<<" "<<record->acct<<" "<<record->hour<<" "<<record->min<<" "<<record->day<<" "<<record->year<<endl;
				return;
			}
			temp_offset = temp->next;
			idx.seek(temp_offset, BEGIN);
			idx.read_raw( (char *) temp, sizeof( ch_node ) );
		}
		if(temp->k == key) {
			db.seek(temp->rec, BEGIN);
			db.read_raw( (char *) record, sizeof( f_rec ) );
			cout<<record->num<<" "<<record->amt<<" "<<record->type<<" "<<record->acct<<" "<<record->hour<<" "<<record->min<<" "<<record->day<<" "<<record->year<<endl;
			return;
		}
		cout<<"Record "<<key<<" does not exist."<<endl;
		
	}
}

void Hash::del(int key) {
	ch_node *temp = new ch_node;
	ch_node *prev = new ch_node;
	long temp_offset, prev_offset;
	int hash_value;
	hash_value = key % TBL_SIZE;
	idx.seek(hash_value * sizeof(ch_node), BEGIN);
	idx.read_raw( (char *) temp, sizeof( ch_node ) );
	if(temp->k == -1) {
		cout<<"Record "<<key<<" does not exist."<<endl;
		return;
	}
	else if(temp->k == key && temp->next == -1) {
		//only 1 record in that hash value, no need of prev
		temp->k = -1;
		idx.seek( hash_value * sizeof(ch_node), BEGIN );
		idx.write_raw( (char *) temp, sizeof( ch_node ) );
	}
	else if(temp->k == key && temp->next != -1) {
		//first record is key, and more records, so make second record the first
		temp_offset = temp->next;
		idx.seek(temp_offset, BEGIN);
		idx.read_raw( (char *) temp, sizeof( ch_node ) );
		idx.seek( hash_value * sizeof(ch_node), BEGIN );
		idx.write_raw( (char *) temp, sizeof( ch_node ) );
	}
	else {
		//hash_value has records, first one is not the key
		
		//prev points to first node
		prev_offset = hash_value * sizeof(ch_node);
		idx.seek(prev_offset, BEGIN);
		idx.read_raw( (char *) prev, sizeof( ch_node ) );
		
		//temp moves to second node
		temp_offset = temp->next;
		idx.seek(temp_offset, BEGIN);
		idx.read_raw( (char *) temp, sizeof( ch_node ) );
		
		while(temp->next != -1) {
			if(temp->k == key) {
				prev->next = temp->next;
				idx.seek(prev_offset, BEGIN);
				idx.write_raw( (char *) prev, sizeof( ch_node ) );
				return;
			}
			prev_offset = prev->next;
			idx.seek(prev_offset, BEGIN);
			idx.read_raw( (char *) prev, sizeof( ch_node ) );
			
			temp_offset = temp->next;
			idx.seek(temp_offset, BEGIN);
			idx.read_raw( (char *) temp, sizeof( ch_node ) );
		}
		if(temp->k == key) {
			prev->next = -1;
			idx.seek(prev_offset, BEGIN);
			idx.write_raw( (char *) prev, sizeof( ch_node ) );
			return;
		}
		cout<<"Record "<<key<<" does not exist."<<endl;
		
	}
}

void Hash::print() {
	long nextoff;
	ch_node *entry = new ch_node;
	for(int i=0; i<TBL_SIZE; i++) {
		idx.seek( ( i * sizeof(ch_node) ), BEGIN);
		idx.read_raw( (char *) entry, sizeof( ch_node ) );
		cout<<i<<": ";
		while(entry->k != -1) {
			cout<<entry->k<<"/"<<entry->rec<<" ";
			if(entry->next != -1) {
				nextoff = entry->next;
				idx.seek(nextoff, BEGIN);
				idx.read_raw( (char *) entry, sizeof( ch_node ) );
			}
			else break;
		}
		cout<<endl;
	}
}

void Hash::end() {
	idx.close();
	db.close();
}

int main(int argc, char** argv) {
	string index_file = argv[1];
	string db_file = argv[2];
	filereader fr;
	string line, part[2];
	fr.open('r');
	Hash h(index_file, db_file);
	
	while(!fr.eof())
	{
		fr.getline(line,1);
		line.token(part,2);
		if(part[0] == "add") {
			h.add(line);
		}
		else if(part[0] == "find") {
			h.find((int) part[1]);
		}
		else if(part[0] == "delete") {
			h.del((int) part[1]);
		}
		else if(part[0] == "print") {
			h.print();
		}
		else if(part[0] == "end") {
			h.end();
			return 0;
		}
	}
	return 0;
}

