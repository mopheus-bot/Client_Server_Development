from pymongo import MongoClient
from bson.objectid import ObjectId
from pprint import PrettyPrinter

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """
    
    def __init__(self, username, password):
        # Initializing the MongoClient. This helps to
        # access the MongoDB databases and collections.
        # This is hard-wired to use the AAC database, the
        # animals collection, and the aac user.
        # Definitions of the connection string variables are
        # unique to the individual Apporto environment.
        #
        # Connection variables:
        #
        USER = username
        PASS = password
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 30628
        DB = 'AAC'
        COL = 'animals'
        #
        # Initialize Connection
        #
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]
        
    # Create method - C in CRUD
    def create(self, data):
        if data is not None:
            self.collection.insert_one(data) # data should be dictionary
            return True
        else:
            raise Exception("Nothing to save, because data parameter is empty")
            return False
    
    # Read method - R in CRUD
    def read(self, key):
        if key is not None:
            #pprinter = PrettyPrinter(indent = 4)
            results = list(self.collection.find(key)) # key should be dictionary
            #return pprinter.pformat(results) # returns results list in a formatted manner (in memory)
            return results
        else:
            raise Exception("Nothing to query, because key parameter is empty")
            return []
        
    # Update method - U in CRUD
    def update(self, key, data):
        if data is not None and key is not None:
            numUpsert = self.collection.update_many(key, {"$set": data}).modified_count # key, data should be dictionary
            return numUpsert
        else:
            raise Exception("Nothing to update, because key/data parameter is empty")
            return 0
        
    # Delete method - D in CRUD
    def delete(self, key):
        if key is not None:
            numDel = self.collection.delete_many(key).deleted_count
            return numDel
        else:
            raise Exception("Nothing to delete, because key parameter is empty")
            return 0
        
    # Aggregation for duplicate documents - one-time process
    def agg(self):
        uniques = []
        delcount = 0
        for entry in self.collection.find({}):
            entry_rec = entry["rec_num"]
            if entry_rec not in uniques:
                uniques.append(entry_rec)
            else:
                self.collection.delete_one({'_id': entry["_id"]})
                delcount += 1
        if delcount == 0:
            raise Exception("Nothing to delete, because all entries are unique")
        return delcount