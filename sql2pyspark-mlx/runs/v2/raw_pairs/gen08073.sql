SELECT id, quantity, product_id
FROM order_items
WHERE quantity IS NOT NULL
INTERSECT
SELECT id, quantity, product_id
FROM order_items
WHERE order_id = 5;
