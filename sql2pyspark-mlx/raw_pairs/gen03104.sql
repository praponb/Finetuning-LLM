SELECT id, event_type, event_time, NTILE(2) OVER (PARTITION BY event_type ORDER BY event_time, id) AS quartile FROM web_events WHERE page IS NULL ORDER BY event_type
