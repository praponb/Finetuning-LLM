SELECT id, page, session_id
FROM web_events
WHERE page IN ('/checkout', '/cart', '/search') AND session_id IN (6, 21)
