select id, device_code from device_readings except all select id, device_code from device_readings where firmware != '1.1.0';
