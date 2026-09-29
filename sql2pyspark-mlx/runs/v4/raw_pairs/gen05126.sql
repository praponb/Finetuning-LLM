SELECT order_id, COUNT(DISTINCT amount) AS unique_orders, MAX(amount) AS max_amount
FROM payments
WHERE paid_at >= '2024-11-01' AND paid_at >= '2025-05-01'
GROUP BY 1;
