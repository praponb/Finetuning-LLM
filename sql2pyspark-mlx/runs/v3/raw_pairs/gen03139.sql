SELECT event_type, AVG(duration_sec) avg_duration_sec, COUNT(duration_sec) duration_sec_count
FROM web_events
WHERE session_id = 24
GROUP BY event_type
HAVING MIN(duration_sec) > 130;
