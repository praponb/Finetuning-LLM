SELECT order_id
FROM order_items
WHERE order_id IN (16, 19, 25, 40)
UNION
SELECT order_id
FROM order_items
WHERE order_id IN (2, 9);
