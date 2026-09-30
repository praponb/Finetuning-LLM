SELECT s.id, COUNT(we.id) AS web_event_count, SUM(CASE WHEN we.page = '/product' THEN 1 ELSE 0 END) AS product_count
FROM sessions s
LEFT OUTER JOIN web_events we ON we.session_id = s.id
GROUP BY s.id
HAVING COUNT(we.id) > 3;
