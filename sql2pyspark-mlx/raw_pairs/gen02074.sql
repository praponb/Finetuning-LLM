SELECT id, duration_sec, page, event_type FROM web_events WHERE duration_sec >= (SELECT AVG(duration_sec) FROM web_events)
