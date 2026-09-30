SELECT id, product_id, quantity
FROM order_items
WHERE unit_price BETWEEN 280 AND 350
UNION
SELECT id, product_id, quantity
FROM order_items
WHERE quantity >= 7
