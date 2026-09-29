SELECT product_id, order_id
FROM order_items
WHERE order_id IN (1, 2, 17, 24)
UNION ALL
SELECT product_id, order_id
FROM order_items
WHERE order_id IN (1, 10)
ORDER BY order_id DESC
