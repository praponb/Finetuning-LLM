SELECT id, event_type, event_time, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS rn FROM web_events WHERE duration_sec < 367 ORDER BY rn ASC, id DESC;
