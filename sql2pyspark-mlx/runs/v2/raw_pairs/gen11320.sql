SELECT o.channel, MAX(p.amount) AS max_amount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
WHERE YEAR(o.order_date) >= 2025
GROUP BY o.channel
ORDER BY channel
LIMIT 5
