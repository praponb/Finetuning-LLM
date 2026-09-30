SELECT o.status, COUNT(oi.id) AS order_item_count, MAX(oi.quantity) AS max_quantity
FROM orders o
INNER JOIN order_items oi ON oi.order_id = o.id
GROUP BY o.status
