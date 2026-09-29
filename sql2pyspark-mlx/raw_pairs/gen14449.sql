SELECT HOUR(event_time) AS event_hour, MIN(event_time) AS first_event_time, COUNT(duration_sec) AS duration_sec_count
FROM web_events
WHERE session_id IN (7, 24, 27, 34) AND TO_DATE(event_time) < '2025-03-08'
GROUP BY HOUR(event_time)
HAVING COUNT(duration_sec) < 5
