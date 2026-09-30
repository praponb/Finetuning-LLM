SELECT *
FROM payments
WHERE NOT (order_id IS NOT NULL AND paid_at >= '2024-04-01');
