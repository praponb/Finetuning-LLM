SELECT id, device_code, temp_c, pressure, firmware FROM device_readings QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) = 1
