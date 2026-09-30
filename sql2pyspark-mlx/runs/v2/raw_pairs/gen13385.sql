SELECT event_type, session_id, page
FROM web_events
WHERE page = '/product' AND page IN ('/search', '/checkout', '/product')
ORDER BY event_type DESC, session_id;
