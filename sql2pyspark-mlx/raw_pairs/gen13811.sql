WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(pressure) OVER (ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS pressure_avg_2_prior,
        AVG(humidity) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS humidity_avg_15m
    FROM device_readings
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        pressure_avg_2_prior,
        humidity_avg_15m,
        row_num,
        CASE WHEN temp_c >= 35.9 THEN 'High' WHEN temp_c >= 22 THEN 'Medium' ELSE 'Low' END AS temp_c_level
    FROM deduped
    WHERE pressure_avg_2_prior > 1016
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    MIN(reading_ts) AS started_at,
    SUM(humidity_avg_15m) AS total_humidity_avg_15m,
    MIN(temp_c) AS min_temp_c,
    ROUND(AVG(session_id), 2) AS avg_session_id
FROM sessions_device_readings
GROUP BY DATE_TRUNC('DAY', reading_ts)
