SELECT oi.id, oi.quantity, oi.product_id
FROM order_items oi
WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE discount < 0.38);
