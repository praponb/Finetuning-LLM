SELECT status
FROM payments
WHERE order_id IN (8, 10, 16, 19)
UNION ALL
SELECT status
FROM payments
WHERE status IN ('REFUNDED', 'SUCCESS', 'FAILED');
