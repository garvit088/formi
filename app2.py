from flask import Flask, request, jsonify
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from fuzzywuzzy import process, fuzz
import pandas as pd
import threading
import time

app = Flask(__name__)

# Load properties
df = pd.read_csv("data.csv")
properties = []

stopwords = {"moustache", "luxuria", "hostel", "resort", "retreat", "the", "riverside", "camp", "verandah"}

# Build property token list (not for final fallback)
city_tokens = set()
for _, row in df.iterrows():
    name = row["property"]
    lat = row["latitude"]
    lon = row["longitude"]
    properties.append({"property_name": name, "latitude": lat, "longitude": lon})

    tokens = [word.lower() for word in name.split() if word.lower() not in stopwords]
    city_tokens.update(tokens)

known_tokens = list(city_tokens)
geolocator = Nominatim(user_agent="moustache-geoapi", timeout=5)


def geocode_location(text, timeout=1.5):
    result_holder = {}

    def thread_target():
        try:
            result = geolocator.geocode(text, addressdetails=True)
            if result and "India" in result.raw.get("address", {}).get("country", ""):
                result_holder["location"] = (result.latitude, result.longitude)
                result_holder["name"] = result.raw.get("display_name", text)
        except:
            pass

    thread = threading.Thread(target=thread_target)
    thread.start()
    thread.join(timeout)
    return result_holder.get("location"), result_holder.get("name")


@app.route("/nearest-properties", methods=["GET"])
def nearest_properties():
    start_time = time.time()
    query = request.args.get("location_query")
    if not query:
        return jsonify({"error": "Missing 'location_query' parameter"}), 400

    resolved_location = None
    resolved_name = None

    # Step 1: Try to geocode the raw input (including typos!)
    resolved_location, resolved_name = geocode_location(query)
    
    # Step 2: If geocoding failed, try fuzzy-matching city names from tokens & geocode best match
    if not resolved_location:
        match, score = process.extractOne(query.lower(), known_tokens, scorer=fuzz.ratio)
        if score >= 60:
            resolved_location, resolved_name = geocode_location(match)
            if not resolved_location:
                return jsonify({"error": f"Could not geocode matched location '{match}'"}), 404
        else:
            return jsonify({"error": f"Could not resolve location '{query}'"}), 404

    # Step 3: Find properties within 50km of resolved location
    nearby = []
    for prop in properties:
        dist = geodesic(resolved_location, (prop["latitude"], prop["longitude"])).km
        if dist <= 50:
            nearby.append({
                "property_name": prop["property_name"],
                "distance_km": round(dist, 2)
            })

    response_time_sec = round(time.time() - start_time, 2)

    response = {
        "input_location": query,
        "resolved_location": {
            "name": resolved_name,
            "latitude": resolved_location[0],
            "longitude": resolved_location[1]
        },
        "response_time_sec": response_time_sec
    }

    if nearby:
        response["nearby_properties"] = sorted(nearby, key=lambda x: x["distance_km"])
    else:
        response["message"] = f"No properties found within 50km of '{query}'."

    return jsonify(response)

if __name__ == "__main__":
    app.run(debug=True)
