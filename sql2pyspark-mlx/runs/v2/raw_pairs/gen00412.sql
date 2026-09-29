SELECT oi.id, oi.quantity, oi.product_id, p.price FROM order_items oi JOIN products p ON oi.product_id = p.id;
