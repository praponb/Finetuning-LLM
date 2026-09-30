SELECT id, duration_sec, event_time FROM web_events WHERE duration_sec >= (SELECT MAX(duration_sec) FROM web_events) ORDER BY id DESC, duration_sec LIMIT 10
