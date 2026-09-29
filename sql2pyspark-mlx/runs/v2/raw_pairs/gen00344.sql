SELECT o.channel, MAX(oi.quantity) - MIN(oi.quantity) AS quantity_range
FROM order_items oi
INNER JOIN orders o ON o.id = oi.order_id
INNER JOIN customers c ON c.id = o.customer_id
WHERE oi.order_id IN (17, 19, 20, 27)
GROUP BY o.channel;
