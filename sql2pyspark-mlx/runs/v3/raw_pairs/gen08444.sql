SELECT order_id, MAX(paid_at) AS last_paid_at
FROM payments
WHERE order_id IS NOT NULL AND paid_at < '2025-02-01'
GROUP BY order_id
