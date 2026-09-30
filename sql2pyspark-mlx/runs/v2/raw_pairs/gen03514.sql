SELECT id, session_id FROM web_events WHERE page IN ('/home', '/search') UNION SELECT id, session_id FROM web_events WHERE duration_sec > 433 ORDER BY id
