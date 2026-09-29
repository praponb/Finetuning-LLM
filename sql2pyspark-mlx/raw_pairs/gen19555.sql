select device_code from device_readings except all select device_code from device_readings where temp_c between 24 and 29;
