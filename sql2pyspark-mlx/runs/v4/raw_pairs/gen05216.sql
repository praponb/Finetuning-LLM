SELECT id, order_id, product_id
FROM order_items
WHERE order_id = 9
EXCEPT
SELECT id, order_id, product_id
FROM order_items
WHERE order_id IN (4, 10, 20, 23)
ORDER BY order_id
