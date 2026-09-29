SELECT o.channel, MIN(oi.quantity) AS min_quantity FROM order_items oi INNER JOIN orders o ON o.id = oi.order_id GROUP BY o.channel ORDER BY channel DESC
