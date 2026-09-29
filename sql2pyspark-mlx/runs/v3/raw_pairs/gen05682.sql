SELECT id, event_type, duration_sec, event_time FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS pos FROM web_events) AS r WHERE pos <= 3 ORDER BY id
