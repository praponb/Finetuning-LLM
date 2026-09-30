SELECT id, order_id, method
FROM payments
WHERE order_id IN (25, 31, 35, 39)
UNION ALL
SELECT id, order_id, method
FROM payments
WHERE TO_DATE(paid_at) >= '2025-05-01';
