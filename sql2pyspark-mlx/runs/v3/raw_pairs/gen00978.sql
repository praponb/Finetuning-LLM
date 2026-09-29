SELECT id, product_id, order_id
FROM order_items
WHERE product_id IS NOT NULL
EXCEPT
SELECT id, product_id, order_id
FROM order_items
WHERE order_id IN (5, 7, 31)
