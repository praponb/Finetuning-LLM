SELECT order_id
FROM order_items
WHERE product_id IS NOT NULL
UNION
SELECT order_id
FROM order_items
WHERE unit_price BETWEEN 130 AND 230
