SELECT o.channel, MAX(oi.unit_price) - MIN(oi.unit_price) unit_price_range
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id
GROUP BY o.channel
