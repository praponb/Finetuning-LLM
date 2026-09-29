SELECT o.id, o.channel, COUNT(p.id) AS payment_count
FROM orders o
INNER JOIN payments p ON p.order_id = o.id
GROUP BY o.id, o.channel
ORDER BY o.id DESC
