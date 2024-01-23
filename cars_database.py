from pprint import pprint

import pymongo
from bson import ObjectId
from bson.json_util import dumps

# connect to the AWS instance mongos
client = pymongo.MongoClient("ec2-50-19-68-242.compute-1.amazonaws.com", 27020)

db = client.cars
collection = db["used_cars"]


def driver():
    print("Welcome to the used cars database. You can use this database to find used cars.")
    while True:
        chosen_function = choose_function()
        if chosen_function == "exit" or chosen_function == "quit":
            break
        results = eval("query_function_" + str(chosen_function) + "()")

        if results is not None:
            length = len(list(results.clone()))
            if length != 0:
                for result in results:
                    res = dumps(result)
                    print(res)
                print("Number of results found: " + str(length))
            else:
                print("No results found.")


def choose_function():
    chosen_function = input("""
      1: Find the car by the body type
      2: Find the car by the city where the car is listed
      3: Find the car by its color
      4: Find the car by its brand 
      5: Find the car by its fuel type
      6: Find the car by its horsepower 
      7: Find if the car is new or not
      8: Find the car by its mileage
      9: Find the car by price
      10: Find the car by the year
      11: Find the car by how many days its been on market
      12: Find the car by its listing id
      13: Insert a car listing
      14: Update a car listing
      15: Delete a car listing
      Enter the number of the query you would like to run (type "exit" or "quit" to quit): """)
    return chosen_function


def query_function_1():
    user_input = input(""" 
    SUV / Crossover
    Sedan
    Pickup Truck
    Coupe
    Hatchback
    Convertible
    Wagon
    Minivan
    Select a body type to query for (please type exactly as displayed): """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'body_type': user_input},
                              {'_id': 0, 'body_type': 1, 'year': 1, 'make_name': 1, 'model_name': 1, 'price': 1,
                               'listing_id': 1}).limit(int(user_limit))
    return results


def query_function_2():
    user_input = input(""" 
      Enter a city (with correct capitalization): """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'city': user_input},
                              {'_id': 0, 'city': 1, 'year': 1, 'make_name': 1, 'model_name': 1, 'price': 1,
                               'listing_id': 1}).limit(int(user_limit))
    return results


def query_function_3():
    user_input = input("""Enter a color: """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'listing_color': user_input.upper()},
                              {'_id': 0, 'listing_color': 1, 'year': 1, 'make_name': 1, 'model_name': 1, 'price': 1,
                               'listing_id': 1}).limit(int(user_limit))
    return results


def query_function_4():
    user_input = input(""" 
      Enter a brand (make name): """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'make_name': user_input},
                              {'_id': 0, 'year': 1, 'make_name': 1, 'model_name': 1, 'price': 1,
                               'listing_id': 1}).limit(int(user_limit))
    return results


def query_function_5():
    user_input = input("""
      Bioldiesel
      Compressed Natural Gas
      Diesel
      Electric
      Flex Fuel Vehicle
      Gasoline
      Hybrid
      Select a fuel type to query for (Enter exactly as shown): """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'fuel_type': user_input},
                              {'_id': 0, 'fuel_type': 1, 'year': 1, 'make_name': 1, 'model_name': 1, 'price': 1,
                               'listing_id': 1}).limit(int(user_limit))
    return results


def query_function_6():
    min_hp = input(""" 
        Enter minimum horsepower: """)
    max_hp = input("""
        Enter the maximum horsepower: """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'horsepower': {"$gte": min_hp, "$lte": max_hp}},
                              {'_id': 0, 'horsepower': 1, 'year': 1, 'make_name': 1, 'model_name': 1, 'price': 1,
                               'listing_id': 1}).limit(int(user_limit))
    return results


def query_function_7():
    user_input = input(""" 
      True
      False
      Select the car new option: """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'is_new': user_input},
                              {'_id': 0, 'is_new': 1, 'year': 1, 'make_name': 1, 'model_name': 1, 'price': 1,
                               'listing_id': 1}).limit(int(user_limit))
    return results


def query_function_8():
    min_mileage = input(""" 
        Enter minimum mileage: """)
    max_mileage = input("""
        Enter the maximum mileage: """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'mileage': {"$gte": min_mileage, "$lte": max_mileage}},
                              {'_id': 0, 'mileage': 1, 'year': 1, 'make_name': 1, 'model_name': 1, 'price': 1,
                               'listing_id': 1}).limit(int(user_limit))
    return results


def query_function_9():
    min_price = input(""" 
        Enter minimum price: """)
    max_price = input("""
        Enter the maximum price: """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'price': {"$gte": min_price, "$lte": max_price}},
                              {'_id': 0, 'price': 1, 'year': 1, 'make_name': 1, 'model_name': 1,
                               'listing_id': 1}).limit(int(user_limit))
    return results


def query_function_10():
    user_input = input(""" 
        Enter the car year: 
      """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'year': user_input},
                              {'_id': 0, 'year': 1, 'make_name': 1, 'model_name': 1, 'price': 1,
                               'listing_id': 1}).limit(int(user_limit))
    return results


def query_function_11():
    min_days = input(""" 
        Enter minimum days on market: """)

    max_days = input("""
        Enter the maximum days on market: """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")

    results = collection.aggregate([{"$addFields": {"convertedDaysOnMarket": { "$convert": {"input": "$daysonmarket", "to": "int", "onError": "", "onNull": "" } } } },
                                    {"$match": { "convertedDaysOnMarket": { "$gte": int(min_days), "$lte": int(max_days) } } },
                                    {"$project": { "_id": 0, "daysonmarket": 1, "year": 1, "make_name": 1, "model_name": 1, "price": 1, "listing_id": 1 } },
                                    {"$limit": int(user_limit)}])
    for result in results:
        print(result)


def query_function_12():
    user_input = input(""" 
        Enter the listing id: 
      """)
    user_limit = input("How many results would you like to see? Enter a positive integer (Ex. 1, 10, 239): ")
    results = collection.find({'listing_id': user_input}, {'_id': 0}).limit(int(user_limit))
    return results


# insert function
def query_function_13():
    vin = input("""
        To insert a car listing you will need the following: vin, year, make, model, listing id, price.
        Enter the Vehicle Identification Number: """)
    year = input("Enter the car year: ")
    make = input("Enter the car make: ")
    model = input("Enter the car model: ")
    list_id = input("Enter the car listing id: ")
    price = input("Enter the car price: ")
    collection.insert_one(
        {"vin": vin, "year": year, "make_name": make, "model_name": model, "listing_id": list_id, "price": price})
    results = collection.find({"listing_id": list_id})

    return results


# update function
def query_function_14():
    # 51
    listing_id = input("To update a car listing, enter the listing id:")
    entries = {"vin": "", "back_length": "", "body_type": "", "cabin": "", "city": "", "daysonmarket": "",
               "dealer_zip": "", "description": "", "engine_cylinders": "", "engine_displacement": "",
               "ent": "", "engine_type": "", "exterior_color": "", "franchise_dealer": "", "franchise_make": "",
               "front_legroom": "", "fuel_tank_volume": "", "fuel_type": "", "height": "",
               "horsepower": "", "interior_color": "", "is_new": "", "latitude": "", "length": "", "listing_date": "",
               "listing_color": "", "longitude": "", "main_picture": "", "major_options": "",
               "make_name": "", "maximum_seating": "", "mileage": "", "model_name": "", "owner_count": "", "power": "",
               "price": "", "savings_amount": "", "seller_rating": "", "sp_id": "",
               "sp_name": "", "theft_title": "", "torque": "", "transmission": "", "transmission_display": "",
               "trimId": "", "trim_name": "", "wheel_system": "", "wheel_system_display": "",
               "wheelbase": "", "width": "", "year": ""}
    results = collection.find({"listing_id": listing_id})
    if len(list(results.clone())) == 0:
        print("Update Failed. Invalid listing id.")
    else:
        for column_name in entries:
            entry_input = input("Enter the " + column_name + " (enter nothing to skip): ")
            if entry_input != "":
                entries[column_name] = entry_input
        if entries["year"] == "" or entries["make_name"] == "" or entries["model_name"] == "":
            print("Update Failed. The year, make, and model is required to update a listing.")
        else:

            myList = list(results.clone())
            if myList:
                oid = ObjectId(str(myList[0]['_id']))
                updated = collection.update_one({"_id": oid, "year": str(myList[0]["year"]), "make_name": str(myList[0]["make_name"]), "model_name": str(myList[0]["model_name"])},
                                                 {"$set": {k: v for k, v in entries.items() if v != ""}})
                print(updated)
                print("Successfully updated. \nListing Id: " + listing_id + "\nObjectId: " + str(oid))
            else:
                print("Update Failed.")


# delete function
def query_function_15():
    listing_id = input("To delete a car listing, enter the listing id: ")
    results = collection.find({"listing_id": listing_id})

    myList = list(results.clone())
    if myList:
        oid = ObjectId(str(myList[0]['_id']))
        deleted = collection.delete_one({"_id": oid})
        print("Successfully deleted. \nListing Id: " + listing_id + "\nObjectId: " + str(oid))
    else:
        print("Delete Failed.")


if __name__ == '__main__':
    driver()
