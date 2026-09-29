SELECT o.channel, MIN(p.amount) AS min_amount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
WHERE p.status IN ('SUCCESS', 'FAILED', 'REFUNDED')
GROUP BY o.channel
ORDER BY min_amount, o.channel
LIMIT 10;
