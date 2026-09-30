SELECT o.customer_id, s.region
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
WHERE o.status = 'CANCELLED' OR s.sqft IS NOT NULL
