SELECT status
FROM payments
INTERSECT
SELECT status
FROM shipments;
