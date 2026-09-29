SELECT oi.id, oi.unit_price FROM order_items oi WHERE oi.quantity < 5 AND oi.order_id IN (SELECT id FROM orders WHERE discount <= 0.24);
