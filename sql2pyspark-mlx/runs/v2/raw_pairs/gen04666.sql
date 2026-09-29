SELECT id, product_id, quantity
FROM order_items
WHERE order_id IN (15, 37)
UNION
SELECT id, product_id, quantity
FROM order_items
WHERE order_id IN (16, 30)
