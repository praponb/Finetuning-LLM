SELECT o.channel, SUM(CASE WHEN p.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count, MAX(p.amount) AS max_amount
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
GROUP BY o.channel
HAVING MAX(p.amount) >= 1430
ORDER BY max_amount DESC;
