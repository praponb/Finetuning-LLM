SELECT id, event_type, duration_sec, RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS duration_sec_rank FROM web_events WHERE event_type = 'search';
