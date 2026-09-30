SELECT id, event_type, event_time FROM (SELECT id, event_type, event_time, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS pos FROM web_events) AS t WHERE pos = 1
