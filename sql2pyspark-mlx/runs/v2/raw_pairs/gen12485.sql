SELECT oi.id, oi.unit_price, oi.product_id
FROM order_items oi
WHERE oi.order_id = 26 AND oi.product_id NOT IN (SELECT id FROM products WHERE brand = 'Acme')
ORDER BY oi.product_id, id
LIMIT 5
