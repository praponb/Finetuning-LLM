SELECT page, session_id FROM web_events WHERE page = '/cart' INTERSECT SELECT page, session_id FROM web_events WHERE session_id = 19 ORDER BY page DESC
