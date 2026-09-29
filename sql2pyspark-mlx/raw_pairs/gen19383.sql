SELECT * FROM device_readings QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC) = 1
