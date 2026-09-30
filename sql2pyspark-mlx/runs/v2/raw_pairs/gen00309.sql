SELECT id, duration_sec, session_id FROM web_events WHERE event_type IN ('search', 'click', 'purchase') UNION SELECT id, duration_sec, session_id FROM web_events WHERE page = '/cart'
