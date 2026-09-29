SELECT oi.id, oi.quantity, oi.product_id FROM order_items oi WHERE oi.product_id IN (SELECT id FROM products WHERE ARRAY_CONTAINS(tags, 'premium'))
