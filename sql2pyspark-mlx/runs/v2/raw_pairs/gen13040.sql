SELECT id, session_id, page, event_time FROM web_events WHERE page = '/search' AND (session_id = 36 OR TO_DATE(event_time) >= '2025-01-01')
