SELECT status
FROM payments
WHERE status IN ('FAILED', 'REFUNDED')
UNION
SELECT status
FROM shipments;
