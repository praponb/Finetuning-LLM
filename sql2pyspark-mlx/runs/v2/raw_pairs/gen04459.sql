SELECT oi.id, oi.unit_price, oi.product_id, oi.quantity FROM order_items oi WHERE oi.quantity BETWEEN 4 AND 7 AND oi.product_id IN (SELECT id FROM products WHERE category = 'Books');
