SELECT oi.id, oi.unit_price FROM order_items oi WHERE oi.product_id NOT IN (SELECT id FROM products WHERE product_name IS NOT NULL) ORDER BY id;
