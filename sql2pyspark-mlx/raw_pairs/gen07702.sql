SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY reading_ts DESC, id) AS event_seq,
    RANK() OVER (ORDER BY pressure) AS pressure_rank,
    CORR(status_bits, pressure) OVER (PARTITION BY MONTH(reading_ts)) AS corr_status_bits_pressure,
    SUM(humidity) OVER (PARTITION BY MONTH(reading_ts), device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS humidity_sum_2h,
    AVG(pressure) OVER (ORDER BY humidity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS pressure_avg_6_prior
FROM device_readings
