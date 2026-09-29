SELECT event_type, duration_sec, HOUR(event_time) AS event_time_hour
FROM web_events
WHERE HOUR(event_time) BETWEEN 7 AND 13 AND event_time < '2025-08-19'
ORDER BY event_type
