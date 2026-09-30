SELECT orders.id, orders.status, COUNT(order_items.id) AS order_item_count
FROM orders
LEFT OUTER JOIN order_items ON order_items.order_id = orders.id
GROUP BY orders.id, orders.status
HAVING COUNT(order_items.id) >= 5;
