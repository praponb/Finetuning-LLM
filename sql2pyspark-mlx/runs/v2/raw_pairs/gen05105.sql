SELECT session_id, page
FROM web_events
WHERE session_id = 3 AND event_time >= '2024-07-01'
ORDER BY page DESC, 1 DESC;
