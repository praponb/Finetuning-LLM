SELECT orders.channel, SUM(order_items.quantity * order_items.unit_price) revenue
FROM order_items
LEFT OUTER JOIN orders ON order_items.order_id = orders.id
WHERE orders.store_id = 10
GROUP BY orders.channel
ORDER BY channel
LIMIT 5;
