SELECT event_time, duration_sec
FROM web_events
WHERE duration_sec IS NOT NULL AND HOUR(event_time) BETWEEN 8 AND 13
ORDER BY duration_sec DESC, 1 DESC
