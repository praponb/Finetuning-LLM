SELECT s.city, AVG(o.shipping_fee) AS avg_shipping_fee, COUNT(DISTINCT o.amount) AS unique_stores
FROM orders o
FULL OUTER JOIN stores s ON o.store_id = s.id
WHERE o.status IN ('CANCELLED', 'PENDING')
GROUP BY s.city
HAVING COUNT(DISTINCT o.amount) > 5;
