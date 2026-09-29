SELECT order_id, status
FROM payments
WHERE amount > 700
UNION
SELECT order_id, status
FROM payments
WHERE paid_at < '2025-05-05'
ORDER BY status;
