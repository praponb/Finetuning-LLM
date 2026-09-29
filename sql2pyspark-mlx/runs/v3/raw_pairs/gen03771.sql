SELECT MIN(duration_sec) AS min_duration_sec, AVG(duration_sec) AS avg_duration_sec FROM web_events WHERE session_id IN (35, 38) ORDER BY avg_duration_sec ASC;
