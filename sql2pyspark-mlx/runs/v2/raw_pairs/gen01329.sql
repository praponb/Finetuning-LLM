SELECT event_type, ROUND(AVG(duration_sec), 2) AS avg_duration_sec FROM web_events WHERE session_id IN (12, 28) GROUP BY 1 ORDER BY avg_duration_sec, event_type LIMIT 1;
