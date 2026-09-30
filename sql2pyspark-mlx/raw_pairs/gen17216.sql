SELECT id, event_type, duration_sec, AVG(duration_sec) OVER (PARTITION BY event_type) event_type_avg_duration_sec FROM web_events WHERE page IS NOT NULL;
