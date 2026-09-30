SELECT paid_at, order_id, DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label FROM payments WHERE amount BETWEEN 1450 AND 1550 AND paid_at >= '2024-08-01'
