SELECT oi.id, oi.product_id, oi.order_id
FROM order_items oi
WHERE oi.order_id IN (SELECT id FROM orders WHERE YEAR(order_date) = 2024)
ORDER BY order_id DESC, id
LIMIT 1
