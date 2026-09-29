SELECT id, duration_sec, RANK() OVER (ORDER BY duration_sec) AS rnk
FROM web_events
WHERE page IN ('/cart', '/home')
