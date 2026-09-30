SELECT product_id, quantity
FROM order_items
WHERE order_id IN (19, 40)
UNION
SELECT product_id, quantity
FROM order_items
WHERE order_id IN (18, 27)
