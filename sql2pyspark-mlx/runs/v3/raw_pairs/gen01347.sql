SELECT s.id, s.country
FROM sessions s
WHERE HOUR(s.started_at) BETWEEN 7 AND 13 AND s.id IN (SELECT session_id FROM web_events WHERE page IN ('/search', '/product', '/cart'))
ORDER BY id DESC
