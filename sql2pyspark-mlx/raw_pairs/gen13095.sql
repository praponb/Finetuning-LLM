WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        FIRST_VALUE(pressure, TRUE) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS LAST) AS temp_c_cume_dist
    FROM device_readings
    WHERE reading_ts < '2023-05-07'
)
SELECT
    id,
    device_code,
    temp_c_cume_dist
FROM metrics
ORDER BY temp_c_cume_dist NULLS LAST
