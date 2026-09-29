SELECT * FROM (SELECT event_type, COUNT(duration_sec) AS duration_sec_count FROM web_events WHERE session_id = 3 GROUP BY event_type) AS x WHERE duration_sec_count >= 6
