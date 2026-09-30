SELECT event_type, COUNT(duration_sec) AS duration_sec_count, AVG(duration_sec) AS avg_duration_sec FROM web_events GROUP BY event_type
