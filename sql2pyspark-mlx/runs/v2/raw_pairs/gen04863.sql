SELECT o.channel, COUNT(*) AS num_order_items
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
GROUP BY o.channel;
