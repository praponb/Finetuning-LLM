SELECT order_id
FROM order_items
INTERSECT
SELECT order_id
FROM order_items
WHERE unit_price > 390
