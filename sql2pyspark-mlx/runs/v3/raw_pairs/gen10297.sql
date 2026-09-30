SELECT o.channel, MAX(oi.unit_price) AS max_unit_price, ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE c.city IN ('London', 'Berlin', 'Singapore')
GROUP BY o.channel
ORDER BY avg_unit_price DESC, channel DESC
