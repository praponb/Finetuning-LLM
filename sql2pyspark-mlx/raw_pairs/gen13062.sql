WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(humidity * 100, 2) AS humidity_scaled
    FROM device_readings
    WHERE temp_c <= 21
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    session_id,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    SUM(CASE WHEN device_code = 'DV-03' THEN temp_c ELSE 0 END) AS dv_03_temp_c,
    MAX(reading_ts) AS last_seen,
    COUNT(*) AS event_count
FROM sessionized
GROUP BY session_id, DATE_TRUNC('DAY', reading_ts)
