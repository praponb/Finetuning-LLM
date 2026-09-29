SELECT order_id, quantity
FROM order_items
WHERE product_id IS NULL
ORDER BY quantity, order_id DESC
