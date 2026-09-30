SELECT device_code, firmware FROM device_readings EXCEPT ALL SELECT device_code, firmware FROM device_readings WHERE humidity > 70
