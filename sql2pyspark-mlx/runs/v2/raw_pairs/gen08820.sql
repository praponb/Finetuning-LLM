SELECT oi.id, oi.order_id, oi.unit_price FROM order_items oi WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE amount <= 1240) ORDER BY unit_price DESC
