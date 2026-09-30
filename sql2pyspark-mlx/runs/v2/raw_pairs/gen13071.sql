SELECT AVG(duration_sec) AS avg_duration_sec
FROM web_events
WHERE session_id IN (21, 23, 29)
