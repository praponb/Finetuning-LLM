SELECT o.id, o.channel, COUNT(p.id) AS payment_count
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id
WHERE o.store_id IS NULL
GROUP BY o.id, o.channel
HAVING payment_count >= 5;
