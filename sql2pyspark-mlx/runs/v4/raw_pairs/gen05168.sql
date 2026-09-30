WITH base AS (
    SELECT
        *,
        CASE WHEN firmware = '2.0.1' THEN 1 ELSE 0 END AS is_2_0_1,
        CAST(status_bits AS DOUBLE) AS status_bits_str
    FROM device_readings
    WHERE TO_DATE(reading_ts) < '2023-07-14'
)
SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY pressure NULLS LAST) AS pressure_percent_rank,
    ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share,
    SUM(status_bits) OVER (ORDER BY is_2_0_1, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_bits
FROM base
