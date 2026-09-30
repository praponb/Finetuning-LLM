SELECT id, event_type
FROM web_events
WHERE page = '/search'
EXCEPT
SELECT id, event_type
FROM web_events
WHERE page NOT IN ('/product', '/checkout')
