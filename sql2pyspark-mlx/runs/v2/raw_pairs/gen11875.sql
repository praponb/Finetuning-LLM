SELECT duration_sec
FROM web_events
WHERE page IN ('/product', '/checkout', '/search')
UNION ALL
SELECT duration_sec
FROM web_events
WHERE duration_sec != 136
