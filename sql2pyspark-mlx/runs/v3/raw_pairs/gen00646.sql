SELECT s.region, COUNT(DISTINCT o.discount) AS unique_customers, COUNT(CASE WHEN o.status = 'CANCELLED' THEN 1 END) AS cancelled_count
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
GROUP BY s.region
ORDER BY 2;
