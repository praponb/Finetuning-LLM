SELECT oi.id, oi.unit_price
FROM order_items oi
WHERE oi.order_id IN (14, 16, 34) AND oi.product_id IN (SELECT id FROM products WHERE supplier_id IN (4, 19, 36, 40))
