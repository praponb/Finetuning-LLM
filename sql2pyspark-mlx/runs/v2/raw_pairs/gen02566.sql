SELECT event_type, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
FROM web_events
WHERE session_id IN (19, 40) AND duration_sec BETWEEN 234 AND 462
GROUP BY event_type
