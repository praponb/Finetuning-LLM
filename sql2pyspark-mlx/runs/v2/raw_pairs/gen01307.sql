SELECT s.city, s.region, COUNT(o.amount) AS amount_count
FROM orders o
LEFT JOIN stores s ON s.id = o.store_id
INNER JOIN customers c ON c.id = o.customer_id
WHERE LOWER(s.store_name) LIKE '%iam%'
GROUP BY s.city, s.region
ORDER BY 3 DESC
