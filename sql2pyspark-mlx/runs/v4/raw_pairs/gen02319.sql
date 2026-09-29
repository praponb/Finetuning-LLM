SELECT
    *,
    SUM(status_bits) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_bits_sum_2h,
    SUM(pressure) OVER w_grp AS pressure_sum_15m,
    ROW_NUMBER() OVER (ORDER BY temp_c DESC, id) AS event_seq,
    ROUND(pressure / SUM(pressure) OVER (PARTITION BY device_code), 4) AS pressure_share
FROM device_readings
WINDOW w_grp AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
