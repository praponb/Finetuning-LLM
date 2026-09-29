SELECT id, quantity
FROM order_items
WHERE order_id IN (13, 21, 27, 32)
EXCEPT
SELECT id, quantity
FROM order_items
WHERE order_id IN (7, 14, 27)
