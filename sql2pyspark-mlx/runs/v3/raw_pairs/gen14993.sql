SELECT event_type, total_duration_sec FROM (SELECT event_type, SUM(duration_sec) AS total_duration_sec FROM web_events GROUP BY event_type) AS sub WHERE total_duration_sec < 1800
