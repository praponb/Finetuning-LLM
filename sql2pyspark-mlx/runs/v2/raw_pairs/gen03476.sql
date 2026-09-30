SELECT status
FROM orders
INTERSECT
SELECT status
FROM payments
ORDER BY status DESC;
