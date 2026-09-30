SELECT oi.id, oi.quantity, oi.unit_price
FROM order_items oi
WHERE oi.product_id NOT IN (SELECT id FROM products WHERE ARRAY_CONTAINS(tags, 'premium'))
