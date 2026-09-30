SELECT quantity
FROM order_items
WHERE order_id = 37
INTERSECT
SELECT quantity
FROM order_items
WHERE order_id = 40;
