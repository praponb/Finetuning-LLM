SELECT o.channel, SUM(oi.quantity) AS total_quantity
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
LEFT OUTER JOIN orders o ON oi.order_id = o.id
GROUP BY o.channel
ORDER BY channel DESC
LIMIT 10;
