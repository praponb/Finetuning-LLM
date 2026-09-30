SELECT
    id,
    reading_ts,
    pressure,
    status_bits,
    SUM(pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS pressure_sum_15m,
    ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share,
    SUM(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure,
    CORR(temp_c, pressure) OVER (PARTITION BY device_code) AS corr_temp_c_pressure,
    SUM(humidity) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_humidity
FROM device_readings
