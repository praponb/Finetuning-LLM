SELECT o.channel, COUNT(*) AS num_order_items
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
GROUP BY o.channel
