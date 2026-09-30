SELECT id, duration_sec FROM web_events WHERE duration_sec > (SELECT MAX(duration_sec) FROM web_events WHERE event_type = 'add_to_cart') ORDER BY duration_sec DESC;
