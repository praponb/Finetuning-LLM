SELECT oi.id, oi.product_id, oi.quantity, o.order_date, o.amount, p.product_name FROM order_items oi JOIN orders o ON oi.order_id = o.id JOIN products p ON oi.product_id = p.id;
