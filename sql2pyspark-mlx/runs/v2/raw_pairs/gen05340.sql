SELECT orders.id, orders.status, orders.order_date FROM orders WHERE NOT EXISTS (SELECT 1 FROM payments WHERE payments.order_id = orders.id AND payments.amount >= 1140);
