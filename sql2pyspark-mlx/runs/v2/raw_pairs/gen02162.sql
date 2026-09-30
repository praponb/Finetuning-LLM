SELECT event_time, duration_sec
FROM web_events
WHERE page <> '/home' AND session_id IN (13, 20)
ORDER BY event_time
