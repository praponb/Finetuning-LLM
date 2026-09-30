SELECT id, product_id
FROM order_items
WHERE order_id IN (29, 32, 38)
EXCEPT
SELECT id, product_id
FROM order_items
WHERE product_id = 7
ORDER BY product_id;
