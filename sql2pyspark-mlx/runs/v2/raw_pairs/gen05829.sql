SELECT id, duration_sec
FROM web_events
WHERE session_id IN (9, 27, 38)
INTERSECT
SELECT id, duration_sec
FROM web_events
WHERE page IN ('/product', '/home', '/search')
ORDER BY duration_sec;
