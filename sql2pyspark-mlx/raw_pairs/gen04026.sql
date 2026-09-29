WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
)
-- Running sum of the flag = session number
SELECT
    id,
    session_id,
    event_type,
    event_time,
    duration_sec,
    SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM gaps
