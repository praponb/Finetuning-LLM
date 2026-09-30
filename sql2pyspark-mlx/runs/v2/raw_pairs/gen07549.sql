SELECT oi.id, oi.quantity, oi.order_id, oi.product_id FROM order_items oi WHERE oi.unit_price BETWEEN 150 AND 280 AND oi.product_id NOT IN (SELECT id FROM products WHERE supplier_id IN (14, 29, 35))
