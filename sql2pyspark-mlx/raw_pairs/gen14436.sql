SELECT o.id, o.channel, COUNT(p.id) AS payment_count FROM orders o LEFT JOIN payments p ON o.id = p.order_id WHERE p.paid_at >= '2024-02-01' GROUP BY o.id, o.channel
