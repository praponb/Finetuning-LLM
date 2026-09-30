SELECT status
FROM payments
WHERE status = 'SUCCESS'
EXCEPT
SELECT status
FROM shipments
WHERE cost < 86
