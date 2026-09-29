SELECT cost
FROM shipments
INTERSECT
SELECT amount
FROM orders;
