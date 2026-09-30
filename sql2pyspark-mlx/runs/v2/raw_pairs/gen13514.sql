SELECT o.channel, ROUND(STDDEV(p.amount), 2) stddev_amount, COUNT(DISTINCT p.amount) unique_orders
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
GROUP BY o.channel
