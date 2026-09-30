SELECT oi.id, oi.unit_price, oi.quantity FROM order_items oi WHERE oi.product_id IN (SELECT id FROM products WHERE price <= 570) ORDER BY oi.unit_price, id LIMIT 10
