SELECT id, duration_sec
FROM web_events
WHERE page IN ('/cart', '/checkout', '/home') AND page IN ('/home', '/checkout', '/product')
ORDER BY id, duration_sec;
