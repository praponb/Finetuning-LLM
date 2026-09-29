SELECT id, duration_sec
FROM web_events
WHERE page = '/product'
UNION
SELECT id, duration_sec
FROM web_events
WHERE session_id IN (4, 35)
ORDER BY id DESC;
