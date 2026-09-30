SELECT o.channel, SUM(oi.unit_price * oi.quantity) AS revenue, MAX(oi.quantity) - MIN(oi.quantity) AS quantity_range
FROM order_items oi
LEFT OUTER JOIN products p ON p.id = oi.product_id
LEFT OUTER JOIN orders o ON oi.order_id = o.id
WHERE p.product_name LIKE 'Ba%'
GROUP BY o.channel
ORDER BY channel DESC;
