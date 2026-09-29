SELECT id, duration_sec, event_time, event_type FROM web_events WHERE duration_sec <= (SELECT MIN(duration_sec) FROM web_events) ORDER BY duration_sec DESC, id LIMIT 1;
