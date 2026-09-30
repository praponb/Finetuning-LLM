SELECT status
FROM payments
WHERE amount >= 1040
INTERSECT
SELECT status
FROM shipments
WHERE delivered_date >= '2024-11-01'
ORDER BY status DESC;
