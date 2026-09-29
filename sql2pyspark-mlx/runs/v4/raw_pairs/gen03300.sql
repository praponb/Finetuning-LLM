SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    CORR(status_bits, humidity) OVER (PARTITION BY device_code) AS corr_status_bits_humidity,
    SUM(status_bits) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits
FROM device_readings;
