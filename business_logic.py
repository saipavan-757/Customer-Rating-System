import csv
import pandas as pd
import mysql.connector as my
from mysql.connector import Error

class database():
    def __init__(self):
        self.__host = "database host"
        self.__database="database name"
        self.__user="username"
        self.__password="password for database"
        try:
            self.conn = my.connect(host = self.__host, database = self.__database, user = self.__user, password = self.__password)
            if self.conn.is_connected():
                self.cursor = self.conn.cursor()
                #cursor.execute("CREATE DATABASE employee")
                print("Database Connected Successfully.")
        except Error as e:
            print("Error While Connecting to MySQL", e)
    def __del__(self):
        
        self.cursor.close()
        self.conn.close()
        print("Connection Closed!")
    def db_retrieve(self):
        #conn = my.connect(host = self.__host, database = self.__database, user = self.__user, password = self.__password)
        #cursor = conn.cursor()
        query = 'select * from countries_info'
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        for i in res:
            print(i)

    def insert_countries_info(self):
        try:
            empdata = pd.read_csv('./csv/countries_info.csv', index_col=False, delimiter = ',')
            count = 0
            sql = "INSERT INTO countries_info VALUES (%s,%s)"
            print("Inserting Countries Info, Please Wait", end = '')
            for i,row in empdata.iterrows():
                #here %S means string values 
                self.cursor.execute(sql, tuple(row))
                count+=1
                print(".", end='')
                # the connection is not auto committed by default, so we must commit to save our changes
                self.conn.commit()
            print("\nInserted ",count, "Rows Successfully!")
        except Error as e:
            print("An Error Occured, Unable to Insert!", e)

    def insert_customer_info(self):
        try:
            empdata = pd.read_csv('./csv/customer_info.csv', index_col=False, delimiter = ',')
            empdata['residential_country_cd'] = empdata['residential_country_cd'].fillna('NULL')
            #print(empdata.to_string())
            sql = "INSERT IGNORE INTO customer_info VALUES (%s,%s,STR_TO_DATE(%s, '%d/%m/%Y'))"
            count = 0
            print("Inserting Customer Info, Please Wait", end = '')
            for i,row in empdata.iterrows():
                #here %S means string values 
                self.cursor.execute(sql,tuple(row))
                count+=1
                print(".", end='')
                # the connection is not auto committed by default, so we must commit to save our changes
                self.conn.commit()
            print("\nInserted ",count, "Rows Successfully!")
        except Error as e:
            print("An Error Occured, Unable to Insert!", e)

    def insert_customer_account_info(self):
        try:
            empdata = pd.read_csv('./csv/customer_account_info.csv', index_col=False, delimiter = ',')
            count = 0
            print("Inserting Customer Account Info, Please Wait", end = '')
            sql = "INSERT IGNORE INTO customer_account_info VALUES (%s,%s,STR_TO_DATE(%s, '%d/%m/%Y'))"
            for i,row in empdata.iterrows():
                #here %S means string values 
                self.cursor.execute(sql,tuple(row))
                count+=1
                print(".", end='')
                # the connection is not auto committed by default, so we must commit to save our changes
                self.conn.commit()
            print("\nInserted ",count, "Rows Successfully!")
        except Error as e:
            print("An Error Occured, Unable to Insert!", e)

    def insert_customer_transactions(self):
        try:
            empdata = pd.read_csv('./csv/customer_transactions.csv', index_col=False, delimiter = ',')
            count = 0
            #print("Inserting Customer Transactions, Please Wait", end = '')
            sql = "INSERT IGNORE INTO customer_transactions(transfer_key, account_key, transaction_amount, transaction_type,transaction_location, transaction_date) VALUES (%s,%s,%f,%s,%s,STR_TO_DATE(%s, '%d/%m/%Y'))"
            ls = []
            for i,row in empdata.iterrows():
                ls.append(tuple(row))
            #print(ls[0])
            self.cursor.executemany(sql,ls)
            self.conn.commit()
        except Error as e:
            print("An Error Occured, Unable to Insert!", e)


#Driver
mydb = database()
mydb.insert_countries_info()
mydb.insert_customer_info()
mydb.insert_customer_account_info()
mydb.insert_customer_transactions()