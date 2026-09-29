SELECT o.id, o.shipping_fee FROM orders o WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.quantity BETWEEN 3 AND 6);
