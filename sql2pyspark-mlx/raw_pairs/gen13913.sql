SELECT MAX(event_time) AS last_event_time, COUNT(DISTINCT duration_sec) AS unique_sessions
FROM web_events
ORDER BY last_event_time DESC;
