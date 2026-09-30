SELECT
    id,
    humidity,
    pressure,
    status_bits,
    LAG(humidity, 1) OVER (PARTITION BY TO_DATE(reading_ts), device_code ORDER BY reading_ts, id) AS prev_humidity,
    LEAD(humidity, 1) OVER (PARTITION BY device_code ORDER BY pressure, id) AS next_humidity
FROM device_readings
WHERE pressure < 1013
