SELECT
    event_type,
    MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range,
    MIN(event_time) AS first_event_time,
    COUNT(*) AS num_web_events
FROM web_events
WHERE session_id IN (11, 19, 21, 30)
GROUP BY event_type
