SELECT event_type, MAX(duration_sec) AS max_duration_sec
FROM web_events
GROUP BY event_type
HAVING MAX(duration_sec) < 320;
