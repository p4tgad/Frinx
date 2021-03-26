#! python3
# -*- coding: utf-8 -*-
# @author:	p4tgad
# version:	v1.3
# created:	2021-03-25
# modified:	2021-03-26
# JSON to PostgreSQL
# ==================

## Modules
## =======
import os,sys
import json
import psycopg2

## Current working directory
## =========================
os.chdir(sys.path[0])
CWD=os.getcwd()

## Constants
## =========
JSON_NAME="configClear_v2.json"
JSON_FULLNAME=os.path.join(CWD,JSON_NAME)

BDI="BDI"
LOOPBACK="Loopback"
PORTCHANNEL="Port-channel"
TENGIGABITETHERNET="TenGigabitEthernet"
GIGABITETHERNET="GigabitEthernet"
IGNORE_BDI=True
IGNORE_LOOPBACK=True
IGNORE_PORTCHANNEL=False
IGNORE_TENGIGABITETHERNET=False
IGNORE_GIGABITETHERNET=False

ID="id"
NAME="name"
DESC="description"
CONF="config"
MAXF="max_frame_size"
PORT_ID="port_channel_id"
PORT_NAME="port_channel_name"
MTU="mtu"
LINK="Cisco-IOS-XE-ethernet:channel-group"

HOST="host.docker.internal"
DBNAME="Frinx"
USER="postgres"
PASS="password"
TABLENAME="json1"

## Functions
## =========
def loadData(json_file):
	"Returns json file 'json_file' as python object (dictionary)"
	with open(json_file,"r",encoding="utf-8") as f:
		# data=f.read()					#working (without json module)
		# data=json.loads(f.read())		#working (not necessary)
		data=json.load(f)
	return(data)

def extractData(json_data):
	"Returns list of relevant dictionary entries extracted from 'json_data' python object"
	extracted=[]
	interfaces=json_data["frinx-uniconfig-topology:configuration"]["Cisco-IOS-XE-native:native"]["interface"]
	for interface_group in interfaces:
		if any([interface_group==BDI and IGNORE_BDI,
				interface_group==LOOPBACK and IGNORE_LOOPBACK,
				interface_group==PORTCHANNEL and IGNORE_PORTCHANNEL,
				interface_group==TENGIGABITETHERNET and IGNORE_TENGIGABITETHERNET,
				interface_group==GIGABITETHERNET and IGNORE_GIGABITETHERNET]):
			continue
		
		for interface in interfaces[interface_group]:
			name=interface[NAME]
			fullname=interface_group+str(name)
			
			description=None
			if DESC in interface:
				description=interface[DESC]
			
			max_frame_size=None
			if MTU in interface:
				max_frame_size=interface[MTU]
			
			port_channel_id=None
			port_channel_name=None
			if LINK in interface:
				port_channel_number=interface[LINK]["number"]
				port_channel_name=PORTCHANNEL+str(port_channel_number)
			
			extracted.append({
				NAME:fullname,
				DESC:description,
				CONF:interface,
				MAXF:max_frame_size,
				PORT_NAME:port_channel_name})
	
	return(extracted)

def filterData(json_extracted):
	"Returns list of dictionary entries that need to be updated (with link to port-channel) from 'json_extracted' python object"
	filtered=[item for item in json_extracted if item[PORT_NAME]]
	return(filtered)

def dumpData(json_data,indent=False):
	"Returns string of 'json_data' dictionary formatted as json file type"
	indentBy=None
	if indent:
		indentBy=4
	return(json.dumps(json_data,indent=indentBy))


## Connect to the database
def dbConnectionStart(host=HOST,database=DBNAME,user=USER,password=PASS):
	"Returns tuple of connection and cursor objects of given postgres database"
	## Connection
	connection=psycopg2.connect(host=host,database=database,user=user,password=password)
	## Cursor
	cursor=connection.cursor()
	return(connection,cursor)

## Close the database connection
def dbConnectionClose(connection,cursor):
	"Closes both connection and cursor created by function 'dbConnectionStart'"
	cursor.close()
	connection.close()
	return
	
def runQuery(cursor,query,values=None,show_query=True):
	"Queries the database with 'query' being the string with placeholders and 'values' being tuple of python variables to replace the placeholders."
	"Returns the query result as a python object if applicable, otherwise returns None."
	cursor.execute(query,values)
	if show_query:
		print(showQuery(cursor,query,values))
	try:
		result=cursor.fetchall()
	except Exception as e:
		# print(e)
		result=None
	return(result)

def showQuery(cursor,query,values=None,decode=True,encoding="utf-8"):
	"Returns the query string as it is passed to database"
	final=cursor.mogrify(query,values)
	if decode:
		final=final.decode(encoding).strip()
	return(final)

def commitChanges(connection,force_commit=False):
	"Makes the changes to the database permanent."
	print()
	if force_commit:
		verify="y"
	else:
		verify=input("Commit changes (y/n)? ")
	
	if verify=="y":
		connection.commit()
		print("The changes were committed to the database.")
	else:
		verify=input("No changes were made. Rollback (y/n)? ")
		if verify=="y":
			connection.rollback()
			print("The changes were removed from memory.")
		else:
			print("The changes were kept in memory.")
	return

def createTable(cursor,show_query=False):
	"Creates table if not exists"
	sql_createtable=f"""
CREATE TABLE IF NOT EXISTS {TABLENAME}(
	id SERIAL,
	connection INTEGER,
	name VARCHAR(255) NOT NULL,
	description VARCHAR(255),
	config json,
	type VARCHAR(50),
	infra_type VARCHAR(50),
	port_channel_id INTEGER,
	max_frame_size INTEGER,
	PRIMARY KEY(id),
	UNIQUE(name)
);
"""
	result=runQuery(cursor,sql_createtable,show_query=show_query)
	return(result)

def insertRows(cursor,json_extracted,show_query=False):
	"Inserts rows of data from the file 'json_extracted'"
	insertvalues_placeholder=",\n\t".join("(%s,%s,%s,%s)" for _ in json_extracted)
	
	sql_insertrows=f"""
INSERT INTO {TABLENAME}
	({NAME},{DESC},{CONF},{MAXF})
VALUES
	{insertvalues_placeholder}
ON CONFLICT ({NAME}) DO NOTHING;
"""
	sql_insertrows_values=[]
	for element in json_extracted:
		sql_insertrows_values.extend((element[NAME],element[DESC],dumpData(element[CONF]),element[MAXF]))
	sql_insertrows_values=tuple(sql_insertrows_values)
	
	result=runQuery(cursor,sql_insertrows,sql_insertrows_values,show_query)
	return(result)

def updateRows(cursor,json_filtered,show_query=False):
	"Updates rows to specify port_channel_id where needed"
	updatevalues_placeholder=",\n\t".join(
		f"(%s,(SELECT {ID} FROM {TABLENAME} WHERE {NAME}=%s))" for _ in json_filtered)
	
	sql_updaterows=f"""UPDATE {TABLENAME}
SET {PORT_ID}=v.{PORT_ID}
FROM (VALUES
	{updatevalues_placeholder}
) AS v({NAME},{PORT_ID})
WHERE
	v.{NAME}={TABLENAME}.{NAME}
	AND v.{PORT_ID} IS NULL;
"""
# AND {PORT_ID} IS NULL
	sql_updaterows_values=[]
	for element in json_filtered:
		sql_updaterows_values.extend((element[NAME],element[PORT_NAME]))
	sql_updaterows_values=tuple(sql_updaterows_values)
	
	result=runQuery(cursor,sql_updaterows,sql_updaterows_values,show_query)
	return(result)

def selectRows(cursor,all=False,show_query=False):
	"Selects data from table and prints them"
	if all:
		sql_select=f"SELECT * FROM {TABLENAME} ORDER BY {ID};"
	else:
		sql_select=f"SELECT {ID},{NAME},{DESC},{MAXF},{PORT_ID} FROM {TABLENAME} ORDER BY {ID};"
	
	rows=runQuery(cursor,sql_select,show_query=show_query)
	for element in rows:
		print(element)
	return(rows)


def mainScript():
	"Runs the main script"
	
	## Process json file
	json_data=loadData(JSON_FULLNAME)
	json_extracted=extractData(json_data)
	json_filtered=filterData(json_extracted)
	
	## Connect to database
	conn,curs=dbConnectionStart()
	
	## Run queries
	createTable(curs)
	insertRows(curs,json_extracted)
	updateRows(curs,json_filtered)
	selectRows(curs,show_query=True)
	
	## Store data to database
	commitChanges(conn)
	
	## Close database connection
	dbConnectionClose(conn,curs)
	return


if __name__=="__main__":
	mainScript()



	






	