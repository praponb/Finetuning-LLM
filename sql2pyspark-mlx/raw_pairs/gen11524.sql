SELECT
    id,
    duration_sec,
    event_time,
    page
FROM web_events
WHERE duration_sec >= (SELECT MAX(duration_sec) FROM web_events WHERE session_id IN (5, 19, 20, 30))
