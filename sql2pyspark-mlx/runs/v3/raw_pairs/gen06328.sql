SELECT id, duration_sec, event_time FROM web_events WHERE duration_sec <= (SELECT MIN(duration_sec) FROM web_events WHERE page NOT IN ('/product', '/cart'));
