SELECT oi.id, oi.quantity, oi.product_id FROM order_items oi WHERE oi.product_id NOT IN (SELECT id FROM products WHERE price BETWEEN 700 AND 980);
