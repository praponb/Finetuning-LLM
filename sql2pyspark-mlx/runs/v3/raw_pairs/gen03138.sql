SELECT
    id,
    session_id,
    duration_sec,
    event_time,
    DATE_TRUNC('YEAR', event_time) event_time_period
FROM web_events
WHERE (session_id IN (1, 15, 18, 36) AND event_type = 'click') OR session_id IN (12, 27, 34, 37)
ORDER BY event_time_period
