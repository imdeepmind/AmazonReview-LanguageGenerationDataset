import sqlite3
import pandas as pd
import re
import random
from bs4 import BeautifulSoup

class Process:
    SEQ_LENGTH = 40
    sql_transaction = []
    dataset = []
    cursor_train = None
    cursor_validation = None
    cursor_test = None
    
    # I know in advance that there are 199819620 rows 
    NUM_ROWS = 199819620
    train_size = None
    val_size = None
    test_size = None
    counter = 0
    
    def __init__(self, path, database_dir, split=(0.8, 0.1, 0.1), SEQ_LENGTH=40):
        self.SEQ_LENGTH = SEQ_LENGTH
        
        # Connecting to the train database
        connection_train = sqlite3.connect(database_dir + "/sequence_train.db")
        c = connection_train.cursor()
        
        self.cursor_train = c
        
        # Connecting to the validation database
        connection_validation = sqlite3.connect(database_dir + "/sequence_val.db")
        c = connection_validation.cursor()
        
        self.cursor_validation = c
        
        # Connecting to the test database
        connection_test = sqlite3.connect(database_dir + "/sequence_test.db")
        c = connection_test.cursor()
        
        self.cursor_test = c
        
        train, val, test = split
        if (train + val + test) != 1.0:
            raise ValueError('Invalid split data')
        
        self.train_size = int(self.NUM_ROWS * train)
        self.val_size = int(self.NUM_ROWS * val)
        self.test_size = int(self.NUM_ROWS * test)
        
        print('--Reading the dataset--')
        # Reading the dataset
        data = pd.read_csv(path, sep='\t', error_bad_lines=False)
        
        # Filtering it
        data = data[data['verified_purchase'] == 'Y']
        
        # Selecting reviews with review length > SEQ_LENGTH
        data = data[data['review_body'].str.len() > SEQ_LENGTH]
        
        # Selecting review_body column
        data = data[['review_body']]
        
        # Dropping empty rows
        data = data.dropna()
        
        # Shuffling the data
        data = data.sample(frac=1)
        
        data = data.values
        
        self.dataset = data
    
    def create_table(self):
        self.cursor_train.execute("CREATE TABLE IF NOT EXISTS reviews(review TEXT, next TEXT);")
        self.cursor_validation.execute("CREATE TABLE IF NOT EXISTS reviews(review TEXT, next TEXT);")
        self.cursor_test.execute("CREATE TABLE IF NOT EXISTS reviews(review TEXT, next TEXT);")
        
    def transaction_bldr(self, sql, db):
        self.sql_transaction.append(sql)
        
        if len(self.sql_transaction) > 1000:
            random.shuffle(self.sql_transaction)
            
            if db == 'train':
                self.cursor_train.execute('BEGIN TRANSACTION')
                for s in self.sql_transaction:
                    try:
                        self.cursor_train.execute(s)
                    except Exception as ex:
                        print('Transaction fail ', ex)
                        print('SQL ', s)
                self.cursor_train.execute('commit')
                self.sql_transaction = []
            elif db == 'val':
                self.cursor_validation.execute('BEGIN TRANSACTION')
                for s in self.sql_transaction:
                    try:
                        self.cursor_validation.execute(s)
                    except Exception as ex:
                        print('Transaction fail ', ex)
                        print('SQL ', s)
                self.cursor_validation.execute('commit')
                self.sql_transaction = []
            elif db == 'test':
                self.cursor_test.execute('BEGIN TRANSACTION')
                for s in self.sql_transaction:
                    try:
                        self.cursor_test.execute(s)
                    except Exception as ex:
                        print('Transaction fail ', ex)
                        print('SQL ', s)
                self.cursor_test.execute('commit')
                self.sql_transaction = []
            
    def insertData(self, sequence, nxt, db):
        try:
            sql = "INSERT INTO reviews(review, next) VALUES('{}', '{}');".format(sequence, nxt)
            self.transaction_bldr(sql, db)
        except Exception as e:
            print('Something went wrong when inserting the data into database, ',str(e))
        
    def deEmojify(self,inputString):
        return inputString.encode('ascii', 'ignore').decode('ascii')
    
    def clean_review(self,review):
        # Changing to lowercase
        review = self.deEmojify(review.lower())
        
        # Changing he'll to he will
        review = re.sub(r"i'm", "i am", review)
        review = re.sub(r"aren't", "are not", review)
        review = re.sub(r"couldn't", "counld not", review)
        review = re.sub(r"didn't", "did not", review)
        review = re.sub(r"doesn't", "does not", review)
        review = re.sub(r"don't", "do not", review)
        review = re.sub(r"hadn't", "had not", review)
        review = re.sub(r"hasn't", "has not", review)
        review = re.sub(r"haven't", "have not", review)
        review = re.sub(r"isn't", "is not", review)
        review = re.sub(r"it't", "had not", review)
        review = re.sub(r"hadn't", "had not", review)
        review = re.sub(r"won't", "will not", review)
        review = re.sub(r"can't", "cannot", review)
        review = re.sub(r"mightn't", "might not", review)
        review = re.sub(r"mustn't", "must not", review)
        review = re.sub(r"needn't", "need not", review)
        review = re.sub(r"shouldn't", "should not", review)
        review = re.sub(r"wasn't", "was not", review)
        review = re.sub(r"weren't", "were not", review)
        review = re.sub(r"won't", "will not", review)
        review = re.sub(r"wouldn't", "would not", review)
        
        review = re.sub(r"\'s", " is", review)
        review = re.sub(r"\'ll", " will", review)
        review = re.sub(r"\'ve", " have", review)
        review = re.sub(r"\'re", " are", review)
        review = re.sub(r"\'d", " would", review)
        
        review = re.sub(r"'", " ", review)
        review = re.sub(r'"', " ", review)
        
        # Removing links and other stuffs from string
        review = re.sub(r'''(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))''', '', review, flags=re.MULTILINE)
        
        review = BeautifulSoup(review, "lxml").text
        
        return review

    def process(self):
        for index, review in enumerate(self.dataset):
            if index % 1000 == 0:
                print('--Preprocessing {}th review--'.format(index+1))
            
            review = self.clean_review(review[0])
            
            for k in range(len(review) - self.SEQ_LENGTH):
                # Seleting the sequence
                seq = review[k:self.SEQ_LENGTH + k]
                nxt = review[self.SEQ_LENGTH + k]
                
                if self.counter < self.train_size:
                    self.insertData(seq, nxt, 'train')
                elif self.counter < self.train_size + self.val_size:
                    self.insertData(seq, nxt, 'val')
                elif self.counter < self.train_size + self.val_size + self.test_size:
                    self.insertData(seq, nxt, 'test')

                self.counter += 1
                
process = Process('dataset/02.tsv', 'dataset', (.8, .1, .1), 40)
process.create_table()
process.process()
