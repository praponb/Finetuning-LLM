WITH cleaned AS (
    SELECT *, CONCAT(device_code, ' ', firmware) AS device_code_firmware
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
ranked AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) = 1
),
base AS (
    SELECT
        *,
        pressure + status_bits AS pressure_plus_status_bits,
        pressure + humidity AS pressure_plus_humidity
    FROM ranked
    WHERE device_code NOT IN ('DV-03', 'DV-02', 'DV-01')
),
with_gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
remaining_keys AS (
    SELECT device_code, id
    FROM sessionized
    INTERSECT ALL
    SELECT device_code, id
    FROM device_readings
    WHERE firmware NOT IN ('1.1.0', '2.0.1')
),
verified AS (
    SELECT s.*
    FROM sessionized s
    JOIN remaining_keys k ON s.device_code = k.device_code AND s.id = k.id
),
enriched AS (
    SELECT
        id,
        humidity,
        pressure,
        device_code_firmware,
        pressure_plus_humidity,
        session_start_flag,
        island_id,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY DATE_TRUNC('YEAR', reading_ts)), 4) AS humidity_share,
        MIN(temp_c) OVER (PARTITION BY device_code) AS device_code_min_temp_c,
        MAX(humidity) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS humidity_max_1h
    FROM verified
    WHERE humidity > 65
),
valid AS (
    SELECT *, pressure + humidity_max_1h AS pressure_plus_humidity_max_1h
    FROM enriched
    WHERE session_start_flag = 0
)
SELECT
    id,
    device_code_firmware,
    pressure_plus_humidity,
    humidity_share,
    device_code_min_temp_c,
    humidity_max_1h,
    PERCENT_RANK() OVER (ORDER BY pressure_plus_humidity_max_1h NULLS LAST) AS pressure_plus_humidity_max_1h_percent_rank,
    DENSE_RANK() OVER (ORDER BY device_code_min_temp_c DESC NULLS FIRST) AS device_code_min_temp_c_dense_rank,
    RANK() OVER (ORDER BY pressure) AS pressure_rank,
    CUME_DIST() OVER (ORDER BY humidity_share DESC NULLS FIRST) AS humidity_share_cume_dist
FROM valid;
