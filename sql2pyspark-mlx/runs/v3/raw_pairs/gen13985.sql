SELECT store_id, status
FROM orders
WHERE store_id IS NOT NULL
INTERSECT
SELECT store_id, status
FROM orders
WHERE amount < 590
