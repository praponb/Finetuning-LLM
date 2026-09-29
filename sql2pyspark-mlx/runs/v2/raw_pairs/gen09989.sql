SELECT id, event_type, duration_sec, session_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS rn FROM web_events) AS t WHERE rn = 1 ORDER BY id DESC;
