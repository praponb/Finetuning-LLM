SELECT status, 'order' AS origin
FROM orders
WHERE status IS NOT NULL
UNION
SELECT region, 'store' AS origin
FROM stores
WHERE region IS NOT NULL
