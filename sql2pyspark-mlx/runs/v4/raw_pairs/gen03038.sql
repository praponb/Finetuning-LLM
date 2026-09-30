SELECT oi.id, oi.product_id, oi.order_id, oi.quantity FROM order_items oi WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE discount < 0.22)
