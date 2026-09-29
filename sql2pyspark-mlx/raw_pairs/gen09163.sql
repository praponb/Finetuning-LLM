SELECT oi.id, oi.unit_price FROM order_items oi WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE channel IN ('partner', 'web', 'mobile'));
