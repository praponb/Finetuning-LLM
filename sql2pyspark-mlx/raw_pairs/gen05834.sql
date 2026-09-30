WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        firmware,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    APPROX_PERCENTILE(island_id, 0.95) AS island_id_p95,
    ROUND(AVG(island_id), 2) AS avg_island_id,
    BIT_AND(status_bits) AS status_bits_bit_and
FROM sessions_device_readings
GROUP BY DATE_TRUNC('HOUR', reading_ts)
ORDER BY island_id_p95 DESC NULLS FIRST
