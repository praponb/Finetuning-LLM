SELECT s.city, SUM(CASE WHEN o.channel = 'web' THEN 1 ELSE 0 END) AS web_count
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
GROUP BY s.city
