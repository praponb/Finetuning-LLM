SELECT id, duration_sec, session_id
FROM web_events
WHERE duration_sec <= (SELECT MIN(duration_sec) FROM web_events)
ORDER BY 1 DESC
LIMIT 5;
