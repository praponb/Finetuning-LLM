SELECT o.channel, COUNT(CASE WHEN s.status = 'DELIVERED' THEN 1 END) AS delivered_count FROM shipments s LEFT JOIN orders o ON s.order_id = o.id GROUP BY o.channel ORDER BY o.channel DESC
