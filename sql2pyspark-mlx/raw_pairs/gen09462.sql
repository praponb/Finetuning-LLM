SELECT
    id,
    reading_ts,
    temp_c,
    pressure,
    DENSE_RANK() OVER (PARTITION BY device_code ORDER BY status_bits) AS status_bits_dense_rank,
    ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c, id) AS row_seq,
    CORR(pressure, temp_c) OVER (PARTITION BY device_code) AS corr_pressure_temp_c,
    FIRST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_humidity,
    SUM(pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS pressure_sum_30m
FROM device_readings
