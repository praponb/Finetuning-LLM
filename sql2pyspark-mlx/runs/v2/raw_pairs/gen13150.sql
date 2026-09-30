SELECT id, quantity
FROM order_items
WHERE unit_price >= 140
UNION
SELECT id, quantity
FROM order_items
WHERE unit_price >= 200
