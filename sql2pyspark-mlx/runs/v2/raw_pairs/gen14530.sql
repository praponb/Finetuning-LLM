SELECT o.channel, COUNT(oi.id) AS order_item_count, SUM(oi.quantity) AS total_quantity
FROM orders o
LEFT JOIN order_items oi ON o.id = oi.order_id
WHERE o.amount > 1540
GROUP BY o.channel;
