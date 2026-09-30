SELECT o.channel, COUNT(p.id) AS payment_count FROM orders o JOIN payments p ON p.order_id = o.id GROUP BY o.channel HAVING payment_count > 1 ORDER BY payment_count DESC
