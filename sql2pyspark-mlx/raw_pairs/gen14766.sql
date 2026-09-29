SELECT s.id, s.region, COUNT(o.id) AS order_count
FROM stores s
LEFT JOIN orders o ON o.store_id = s.id
WHERE o.order_date BETWEEN '2023-11-01' AND '2024-01-01'
GROUP BY s.id, s.region
ORDER BY order_count DESC, s.id ASC
