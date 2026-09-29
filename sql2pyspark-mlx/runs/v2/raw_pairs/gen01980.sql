SELECT id, event_type
FROM web_events
WHERE page IN ('/checkout', '/product', '/home')
UNION ALL
SELECT id, event_type
FROM web_events
WHERE event_type = 'purchase'
