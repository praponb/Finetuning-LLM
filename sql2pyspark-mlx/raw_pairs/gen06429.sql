SELECT id, duration_sec FROM web_events WHERE duration_sec >= (SELECT AVG(duration_sec) FROM web_events WHERE page = '/cart') ORDER BY duration_sec, id LIMIT 3;
