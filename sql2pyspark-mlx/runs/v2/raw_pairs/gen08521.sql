SELECT orders.id, orders.discount
FROM orders
WHERE NOT EXISTS (SELECT 1 FROM payments WHERE payments.order_id = orders.id AND payments.paid_at >= '2024-12-01');
