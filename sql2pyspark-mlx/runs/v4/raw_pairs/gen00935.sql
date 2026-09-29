SELECT o.channel, MAX(p.amount) AS max_amount, SUM(p.amount) AS total_amount
FROM payments p
JOIN orders o ON p.order_id = o.id
GROUP BY o.channel
ORDER BY total_amount DESC;
