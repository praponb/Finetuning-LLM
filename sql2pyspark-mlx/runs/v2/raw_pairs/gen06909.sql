SELECT s.customer_id, MAX(we.duration_sec) - MIN(we.duration_sec) AS duration_sec_range, COUNT(CASE WHEN we.page = '/product' THEN 1 END) AS product_count
FROM web_events we
LEFT OUTER JOIN sessions s ON s.id = we.session_id
GROUP BY s.customer_id
