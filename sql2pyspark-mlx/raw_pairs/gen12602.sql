SELECT method, paid_at, SUBSTRING(method, 1, 4) AS method_prefix FROM payments WHERE paid_at >= '2024-02-01' AND paid_at < '2023-05-01' AND order_id IS NOT NULL
