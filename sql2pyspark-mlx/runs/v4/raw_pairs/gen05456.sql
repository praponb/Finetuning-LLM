SELECT status, delivered_date
FROM shipments
WHERE cost > 56
EXCEPT
SELECT status, delivered_date
FROM shipments
WHERE status = 'LOST'
ORDER BY status
