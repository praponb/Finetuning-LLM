SELECT id, method
FROM payments
WHERE order_id IN (5, 16)
UNION ALL
SELECT id, method
FROM payments
WHERE status IN ('SUCCESS', 'FAILED')
ORDER BY id
