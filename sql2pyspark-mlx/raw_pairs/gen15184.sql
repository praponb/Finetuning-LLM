WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) AS prev_reading_ts
    FROM device_readings
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
)
-- Running sum of the flag = session number
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    pressure,
    firmware,
    SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM flagged
