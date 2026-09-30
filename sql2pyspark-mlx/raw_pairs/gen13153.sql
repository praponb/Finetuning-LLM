SELECT firmware, device_code
FROM device_readings
EXCEPT
SELECT firmware, device_code
FROM device_readings
WHERE reading_ts >= '2023-08-03'
