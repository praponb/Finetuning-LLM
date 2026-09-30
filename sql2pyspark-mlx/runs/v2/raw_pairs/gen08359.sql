SELECT o.channel, SUM(oi.unit_price) total_unit_price, MAX(oi.unit_price) - MIN(oi.unit_price) unit_price_range
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE p.price < 1010
GROUP BY o.channel
ORDER BY unit_price_range
