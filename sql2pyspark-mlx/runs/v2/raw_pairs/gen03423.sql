SELECT event_type, COUNT(duration_sec) AS duration_sec_count FROM web_events GROUP BY event_type HAVING COUNT(duration_sec) < 1 ORDER BY duration_sec_count DESC;
