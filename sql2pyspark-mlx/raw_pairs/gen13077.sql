SELECT
    *,
    LAST_VALUE(temp_c, TRUE) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_temp_c,
    ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS event_seq,
    LEAD(humidity, 1, 0) OVER (ORDER BY status_bits, id) AS next_humidity
FROM device_readings
