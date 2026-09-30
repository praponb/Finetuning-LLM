WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    island_id,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    ROUND(AVG(island_id), 2) AS avg_island_id,
    COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows,
    SUM(status_bits) FILTER (WHERE device_code = 'DV-02') AS dv_02_total_status_bits
FROM islands
GROUP BY 1, 2
ORDER BY dv_02_rows NULLS FIRST;
