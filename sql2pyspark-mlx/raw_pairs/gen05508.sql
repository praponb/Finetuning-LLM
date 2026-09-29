WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
)
-- Running sum of the flag = session number
SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM gaps
