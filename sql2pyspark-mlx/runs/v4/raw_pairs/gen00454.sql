SELECT id, device_code FROM device_readings INTERSECT SELECT id, device_code FROM device_readings WHERE status_bits BETWEEN 7 AND 11
