SELECT o.channel, COUNT(p.id) AS payment_count FROM orders o LEFT JOIN payments p ON p.order_id = o.id WHERE o.discount <= 0.13 GROUP BY o.channel;
