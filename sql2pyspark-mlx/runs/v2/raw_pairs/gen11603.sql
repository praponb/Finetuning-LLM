SELECT oi.id, oi.quantity FROM order_items oi WHERE oi.product_id NOT IN (SELECT id FROM products WHERE SIZE(tags) >= 2);
