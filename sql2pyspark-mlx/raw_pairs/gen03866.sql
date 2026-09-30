SELECT oi.id, oi.quantity FROM order_items oi WHERE oi.order_id IN (SELECT id FROM orders WHERE shipping_fee IS NULL);
