WITH session_stats AS (
    SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, MIN(event_time) AS min_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.customer_id,
    ss.duration_sec_range,
    ss.min_event_time
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id
ORDER BY min_event_time ASC, ss.duration_sec_range DESC
