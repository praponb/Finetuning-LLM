SELECT id, duration_sec FROM web_events WHERE duration_sec <= (SELECT MIN(duration_sec) FROM web_events) ORDER BY id DESC, duration_sec DESC LIMIT 50;
