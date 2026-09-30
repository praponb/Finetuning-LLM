WITH with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) AS prev_event_time
    FROM web_events
),
flagged AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
)
SELECT
    id,
    event_type,
    page,
    event_time,
    duration_sec,
    SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM flagged
