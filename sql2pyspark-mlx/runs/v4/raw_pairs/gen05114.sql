SELECT order_id, quantity
FROM order_items
WHERE quantity > 4 AND order_id IN (4, 15, 29) AND order_id IN (8, 11, 26, 31)
