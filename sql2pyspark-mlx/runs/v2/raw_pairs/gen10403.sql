SELECT orders.id, orders.discount, orders.status
FROM orders
WHERE NOT EXISTS (SELECT 1 FROM payments WHERE payments.order_id = orders.id AND HOUR(payments.paid_at) BETWEEN 12 AND 15);
