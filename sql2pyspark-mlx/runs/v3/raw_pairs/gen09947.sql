SELECT order_id
FROM order_items
WHERE product_id IS NULL
UNION ALL
SELECT order_id
FROM order_items
WHERE quantity <= 3
ORDER BY order_id DESC
