SELECT o.channel, SUM(oi.unit_price) AS total_unit_price
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id
GROUP BY o.channel
