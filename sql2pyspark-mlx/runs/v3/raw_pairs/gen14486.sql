SELECT id, page, duration_sec
FROM web_events
WHERE event_time >= '2023-07-01'
UNION
SELECT id, page, duration_sec
FROM web_events
WHERE session_id IN (6, 18);
