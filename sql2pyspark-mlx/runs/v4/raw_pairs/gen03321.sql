SELECT status
FROM orders
WHERE customer_id IS NOT NULL
INTERSECT
SELECT status
FROM shipments
WHERE cost IS NOT NULL;
