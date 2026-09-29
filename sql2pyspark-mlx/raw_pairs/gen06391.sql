SELECT status
FROM orders
INTERSECT ALL
SELECT status
FROM orders
WHERE store_id IS NULL;
