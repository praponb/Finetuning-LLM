SELECT payments.id, payments.order_id, payments.paid_at, payments.amount FROM payments WHERE payments.order_id NOT IN (SELECT id FROM orders WHERE order_date > '2024-10-01');
