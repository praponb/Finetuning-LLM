WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) AS prev_event_time
    FROM web_events
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
)
-- Running sum of the flag = session number
SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM with_gaps
