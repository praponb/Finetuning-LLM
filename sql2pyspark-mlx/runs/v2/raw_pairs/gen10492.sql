SELECT
    id,
    duration_sec,
    session_id,
    event_type
FROM web_events
WHERE event_type = 'page_view' AND (session_id IN (15, 32, 35) OR event_time >= '2024-05-01')
ORDER BY event_type ASC;
