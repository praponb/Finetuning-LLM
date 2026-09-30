SELECT id, event_type, event_time, duration_sec, duration_sec - LAG(duration_sec, 1) OVER (PARTITION BY event_type ORDER BY event_time, id) AS duration_sec_change FROM web_events;
