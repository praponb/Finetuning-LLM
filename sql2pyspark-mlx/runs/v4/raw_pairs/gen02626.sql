SELECT id, product_id
FROM order_items
WHERE quantity <> 7
INTERSECT
SELECT id, product_id
FROM order_items
WHERE order_id IN (6, 8, 14)
