SELECT o.channel, COUNT(p.id) AS payment_count FROM orders o LEFT JOIN payments p ON o.id = p.order_id WHERE o.channel = 'web' GROUP BY o.channel ORDER BY payment_count DESC
