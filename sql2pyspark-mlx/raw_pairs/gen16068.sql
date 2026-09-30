WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        pressure,
        firmware,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN island_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS island_id_tier,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    ROUND(SUM(island_id) / NULLIF(COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END), 0), 2) AS island_id_per_dv_04,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    COUNT_IF(device_code = 'DV-04') AS dv_04_events,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows
FROM islands
GROUP BY CASE WHEN island_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('HOUR', reading_ts)
HAVING dv_04_events >= 1;
