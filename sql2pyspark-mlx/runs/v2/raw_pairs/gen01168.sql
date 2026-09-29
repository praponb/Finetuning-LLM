SELECT MONTH(event_time) AS event_month, COUNT(duration_sec) AS duration_sec_count
FROM web_events
WHERE session_id = 4
GROUP BY event_month;
