SELECT o.channel, SUM(p.amount) AS total_amount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
GROUP BY o.channel
HAVING SUM(p.amount) >= 3200
ORDER BY channel
LIMIT 5
