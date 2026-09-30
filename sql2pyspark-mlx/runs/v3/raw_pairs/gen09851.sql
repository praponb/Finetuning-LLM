SELECT s.region, ROUND(AVG(o.amount), 2) AS avg_amount
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
WHERE s.sqft < 6575
GROUP BY s.region
ORDER BY s.region
