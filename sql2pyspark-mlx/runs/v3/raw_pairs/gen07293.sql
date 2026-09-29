SELECT quantity, order_id
FROM order_items
WHERE quantity < 4
UNION
SELECT quantity, order_id
FROM order_items
WHERE quantity > 5
ORDER BY quantity;
