SELECT id, event_type, duration_sec, MIN(duration_sec) OVER (PARTITION BY event_type) AS event_type_min_duration_sec FROM web_events
