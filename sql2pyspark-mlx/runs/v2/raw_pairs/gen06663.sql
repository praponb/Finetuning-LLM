SELECT MAX(event_time) AS last_event_time, ROUND(AVG(duration_sec), 2) AS avg_duration_sec FROM web_events ORDER BY avg_duration_sec DESC
