#!/usr/bin/env python3
from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_str = f"mongodb+srv://njoro:{password}@cluster0.aiekeau.mongodb.net/"
client = MongoClient(connection_str)

databases = client.list_database_names()

#access the database
test_db = client.test

#list all the collections in the database
collections = test_db.list_collection_names()
print(collections)

#creating and isnerting a document in a collection
def insert_test_doc():
    #accessing a collection
    collection = test_db.test

    test_doc = {
        "name": "kikie",
        "type": "test"
    }
    """
    insert the doc
    all documents will have an id and its one thing we might want to access in this doc, to access it
    inserted_id = collection.insert_one(test_doc).inserted_id - returns the id its given - -the id is a BSON object
    """
    inserted_id = collection.insert_one(test_doc).inserted_id 
    print(inserted_id)

#creating a database from code
#here we are accessing a database that doesn't exist, mongodb detects that and creates it
production = client.production

#creating a person collection from document/databse production
#since it doesn't exist, mongodb creates it
person_collection = production.person_collection

#insert a document - if you don't insert a document, it won't create the above collection and document
def create_doc():
    first_names = ["Tony", "Kikie", "Kris", "Mitch", "Njoro"]
    last_names = ["local", "Ochengo", "kikie", "kwambo", "man"]
    ages = [21, 22, 23, 24, 25]

    docs = []

    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)

    person_collection.insert_many(docs)

#create_doc()

#QUERYING
#getting all documents

#imported from pprint
printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find() #when find is empty, it finds all the collections
    """print(people)
        print(list(people))
    """

    for person in people:
        printer.pprint(person)

#find_all_people()

#find a single document
#its ideal to search for documents using their id but here we will use its first name, you can also specify multiple values
def find_item():
    #item = person_collection.find_one({"first_name": "Kikie"})
    item = person_collection.find_one({"first_name": "Kikie", "last_name": 'Ochengo'})
    #if instead of find_one you use find, it will return all documents that match that criteria defined
    
    printer.pprint(item)

#find_item()

def count_all_people():
    count = person_collection.count_documents(filter={}) #if filter is left empty, then, it counts everything
    print(f"NUmber of people: {count}")

#count_all_people()

#find a document by id
def get_person_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)

#get_person_by_id("64b501f20035ec84b756451f")

#getting documents in a certain range. here we use range
def get_age_range(min_age, max_age):
    query = {"$and": [
            {"age": {"$gte": min_age}},
            {"age": {"$lte": max_age}}
        ]}

    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

#get_age_range(22, 24)


#retrieving certain columns
def project_columns():
    #0 if you dont want the column to be shown, 1 otherwise
    columns = {"_id": 0, "first_name": 1, "last_name": 1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)
    
#project_columns()

#updating an exsting documnet
def update_person_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    #if the field new_field exists, its overridden if not, a new field is added
    """
    all_updates = {
        "$set": {"new_field": True},
        "$inc": {"age": 1}, #incrementing the age by 1, to inc multiple fields, add them with the values
        "$rename": {"first_name": "first", "last_name": "last"} #renaming a field
    }
    person_collection.update_one({"_id": _id}, all_updates)
    """

    #removing a field
    person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})

#update_person_by_id("64b501f20035ec84b756451f")


#REPLACING A DOCUMENT - but keep the id while changing everything else
def replace_one(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "habesha",
        "last_name": "persian",
        "age": 94
    }
    person_collection.replace_one({"_id": _id}, new_doc)

replace_one("64b501f20035ec84b756451f")

#deleting a adocument
def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person_collection.delete_one({"_id": _id})
    #delete many
    #person_collection.delete_many({"key": value}, {"key2": value2}) 
    #person_collection.delete_many({}) #deletes everything in the collection

#delete_doc_by_id("64b501f20035ec84b7564522")


#RELATIONSHIPS
#EMBED
address = {
    "_id":"64b501f20035ec84b7564522",
    "street": "Bay street",
    "number": 94,
    "city": "San Fransisco",
    "country": "United States",
    "zip": "94107",
    #add a new field - foreign key
    "owner_id": "09876543211345678"
}

def add_address_embed(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    #update the document to contian the new address - for flexibility - the address will be a list since one
    #user can have multiple addresses(key) -set determines its going to be an list
    person_collection.update_one({"_id": _id}, {"$addToSet":{"addresses": address}})

#add_address_embed("64b501f20035ec84b7564521", address)

#we are adding a new collection and are going to add the foreign key to tony's id
def add_address_foreign(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    address = address.copy() #avoid mutation of the address
    address["owner_id"] = person_id #updating the address object key owner_id to tony_id

    address_collection = production.address #adding the colletion address_collection of production database
    address_collection.insert_one(address) #adding the document address to the collection address_collection

add_address_foreign("64b501f20035ec84b756451e", address)















"""
#RELATIONSHIPS
#---------------------------------------------------------------------------
below we have two objects, the person may be live in the address given but we have no way
of knowing they do. thats where relationships come in.
there are a couple ways to relate in document db's.
1: embedding docs - embedding a doc inside of anotehr
#EMBEDDING ADDRESS IN PERSON
#--------------------------------------------------------------------------
person = {
    "_id": "09876543211345678",
    "first_name": "njoro",
    #EMBEDDING
    "address": {
        "_id":"64b501f20035ec84b7564522",
        "street": "Bay street",
        "number": 94,
        "city": "San Fransisco",
        "country": "United States",
        "zip": "94107"
        }
}

#in a situation where the address may belong to multiple people, its not ideal to embed it i another object
#STORING IN DIFFERENT COLLECTIONS (use foreign key)
#-------------------------------------------------------------------------------------------

address = {
    "_id":"64b501f20035ec84b7564522",
    "street": "Bay street",
    "number": 94,
    "city": "San Fransisco",
    "country": "United States",
    "zip": "94107",
    #add a new field - foreign key
    "owner_id": "09876543211345678"
}

#with the foreign key, i can now perform joins
person = {
    "_id": "09876543211345678",
    "first_name": "njoro"
}
"""

    
    
    
    
    
    
    
    
    
    
    
    
    
    
"""
    creating multiple doc here - we will loop through all of them and create docs
    for first_name, last_name, age in zip(first_names, last_names, age):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        person_collection.insert_one(doc)
"""














#access a database
"""
test_db = client.test
test_db_2 = client["test"]
"""