SELECT id, session_id FROM web_events WHERE event_type = 'search' INTERSECT SELECT id, session_id FROM web_events WHERE page NOT IN ('/home', '/cart');
