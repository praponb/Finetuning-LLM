SELECT payments.id, payments.paid_at FROM payments WHERE payments.paid_at >= '2024-11-01' AND payments.order_id IN (SELECT id FROM orders WHERE status = 'RETURNED') ORDER BY payments.paid_at
