SELECT event_type, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range FROM web_events GROUP BY event_type ORDER BY event_type, duration_sec_range;
