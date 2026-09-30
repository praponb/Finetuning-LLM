SELECT AVG(duration_sec) AS avg_duration_sec, MIN(event_time) AS min_event_time, SUM(duration_sec) AS total_duration_sec FROM web_events WHERE session_id IN (5, 13, 24, 40) ORDER BY avg_duration_sec;
