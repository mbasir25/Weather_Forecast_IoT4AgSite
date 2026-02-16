# -*- coding: utf-8 -*-
"""
Created on Thu May  1 13:59:22 2025

@author: samif
"""

import requests
import os
import pygrib
import pandas as pd
import numpy as np
from datetime import datetime
import json

# Function to download and convert the binary files to CSV (only used to generate data for JSON)
def data_get(wd):
    # Ensure the working directory exists, if not, create it along with the subfolder
    if not os.path.exists(wd):
        os.makedirs(wd)  # Create directory if it doesn’t exist
    
    #intermediate_folder = os.path.join(wd, "intermediate_files")
    #if not os.path.exists(intermediate_folder):
     #   os.makedirs(intermediate_folder)  # Create intermediate folder

    # NOAA Base URL (Binary Data)
    start_url_1_to_3 = "https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus/VP.001-003/"  # For first 3 days
    start_url_4_to_7 = "https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus/VP.004-007/"  # For 4-7 days
    
    # Define binary files for both 1-3 days and 4-7 days
    bin_files_1_to_3 = {
        "ds.maxt.bin": "ds.maxt_1to3.bin",
        "ds.mint.bin": "ds.mint_1to3.bin",
        "ds.pop12.bin": "ds.pop12_1to3.bin",
        "ds.wspd.bin": "ds.wspd_1to3.bin"
    }
    
    bin_files_4_to_7 = {
        "ds.maxt.bin": "ds.maxt_4to7.bin",
        "ds.mint.bin": "ds.mint_4to7.bin",
        "ds.pop12.bin": "ds.pop12_4to7.bin",
        "ds.wspd.bin": "ds.wspd_4to7.bin"
    }

    # Download the binary files for the first 3 days
    for bin_file, renamed_file in bin_files_1_to_3.items():
        url = start_url_1_to_3 + bin_file
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            file_path = os.path.join(wd, renamed_file)
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"Downloaded {bin_file} for 1-3 days to {file_path}")
        else:
            print(f"Failed to download {bin_file} (Status: {response.status_code})")

    # Download the binary files for the 4-7 days
    for bin_file, renamed_file in bin_files_4_to_7.items():
        url = start_url_4_to_7 + bin_file
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            file_path = os.path.join(wd, renamed_file)
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"Downloaded {bin_file} for 4-7 days to {file_path}")
        else:
            print(f"Failed to download {bin_file} (Status: {response.status_code})")


# Function to process and generate the JSON structure
def generate_json(wd):
    # Initialize an empty dictionary for the JSON result
    #today_date = "2025-05-20"   #for testing script, not a part
    today_date = datetime.today().strftime('%Y-%m-%d')  # Ensure the date is a string
    json_data = {today_date: {}}

    # Function to process each GRIB file and extract necessary data
    def process_grib_file(bin_file, forecast_type):
        bin_path = os.path.join(wd, bin_file)
        grbs = pygrib.open(bin_path)

        # Loop through all GRIB messages for the forecast
        for grb in grbs:
            # Extract latitudes, longitudes, values, and metadata
            lats, lons = grb.latlons()
            values = grb.values  # Extract actual data values
            print(values)    
            variable_name = grb.name  # Extract the variable name (e.g., wind speed)
            forecast_date = grb.validDate.date()  # Extract the forecast date directly from GRIB file
            units = grb.units
            units = units.replace(" ", "")
            
            if variable_name.lower() == "unknown":
                variable_name = "PoP"
            if units.lower() == "unknown":
                units = "%"
            variable_name = variable_name+'('+units+')'
            
            # Filter the data based on the specified latitude and longitude range
            filter_condition = (lats == 40.491971) & (lons == -86.993235)
            values_filtered = values[filter_condition]

            # Calculate the average value for the filtered region
            avg_value = values_filtered.mean()

            # Ensure the forecast date is in string format (YYYY-MM-DD)
            forecast_date_str = forecast_date.strftime('%Y-%m-%d')

            # Add the forecast data to the JSON structure for today's date
            if forecast_date_str not in json_data[today_date]:
                json_data[today_date][forecast_date_str] = {}

            # Add the variable data
            json_data[today_date][forecast_date_str][variable_name] = round(avg_value, 3)

    # Process 1-3 Days Forecast (First 3 days)
    bin_files_1_to_3 = [
        "ds.maxt_1to3.bin",
        "ds.mint_1to3.bin",
        "ds.pop12_1to3.bin",
        "ds.wspd_1to3.bin"
    ]
    for bin_file in bin_files_1_to_3:
        process_grib_file(bin_file, '1to3')

    # Process 4-7 Days Forecast (Next 4-7 days)
    bin_files_4_to_7 = [
        "ds.maxt_4to7.bin",
        "ds.mint_4to7.bin",
        "ds.pop12_4to7.bin",
        "ds.wspd_4to7.bin"
    ]
    for bin_file in bin_files_4_to_7:
        process_grib_file(bin_file, '4to7')

    # Check if the JSON file already exists
    json_path = os.path.join(wd, "forecasted_weather.json")
    if os.path.exists(json_path):
        # If the JSON file exists, load the existing data and append today's data
        try:
            with open(json_path, 'r') as json_file:
                existing_data = json.load(json_file)

            # Append today's data to the existing data
            existing_data.update(json_data)

            # Save the updated JSON data to file
            with open(json_path, 'w') as json_file:
                json.dump(existing_data, json_file, indent=4)
            print(f"✅ Updated JSON data saved to {json_path}")
        except json.JSONDecodeError:
            print(f"Error: The existing JSON file is corrupted or improperly formatted. A new file will be created.")
            with open(json_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=4)
            print(f"✅ New JSON data saved to {json_path}")
    else:
        # If the JSON file doesn't exist, create a new file
        with open(json_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
        print(f"✅ New JSON data saved to {json_path}")


# Main driver code
if __name__ == "__main__":
    #wd = r'weatherdata/news_refs'
    wd = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'weatherdata/news_refs')# Specify the working directory
    data_get(wd)  # Step 1: Download and convert .bin files to CSV
    generate_json(wd)  # Step 2: Generate and save the JSON file
