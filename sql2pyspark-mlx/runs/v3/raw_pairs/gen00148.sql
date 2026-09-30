SELECT
    id,
    duration_sec,
    session_id,
    page
FROM web_events
WHERE duration_sec >= (SELECT AVG(duration_sec) FROM web_events WHERE event_time < '2024-03-01')
ORDER BY id DESC
LIMIT 50
