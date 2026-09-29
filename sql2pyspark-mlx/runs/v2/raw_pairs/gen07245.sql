SELECT id, quantity, order_id
FROM order_items
WHERE quantity >= (SELECT MAX(quantity) FROM order_items)
