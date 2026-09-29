SELECT s.id, COUNT(o.id) AS order_count
FROM stores s
LEFT OUTER JOIN orders o ON s.id = o.store_id
GROUP BY s.id
ORDER BY 1
LIMIT 3;
