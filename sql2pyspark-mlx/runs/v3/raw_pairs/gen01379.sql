SELECT payments.id, payments.method, payments.status FROM payments WHERE payments.order_id IN (SELECT id FROM orders WHERE status IN ('RETURNED', 'COMPLETED'))
