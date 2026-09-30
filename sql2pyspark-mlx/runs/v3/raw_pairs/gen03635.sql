SELECT session_id FROM web_events WHERE (session_id IN (7, 8, 18) AND TO_DATE(event_time) >= '2025-04-01') OR page IN ('/search', '/product')
