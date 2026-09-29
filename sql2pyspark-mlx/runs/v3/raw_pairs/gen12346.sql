SELECT id, quantity, order_id
FROM order_items
WHERE product_id IN (2, 33)
UNION
SELECT id, quantity, order_id
FROM order_items
WHERE quantity >= 4
ORDER BY quantity DESC;
