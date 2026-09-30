SELECT order_id
FROM order_items
WHERE unit_price < 300
UNION
SELECT order_id
FROM order_items
WHERE unit_price >= 370
ORDER BY order_id
