SELECT status
FROM orders
EXCEPT
SELECT status
FROM shipments
WHERE status = 'LOST';
