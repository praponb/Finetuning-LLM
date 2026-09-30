SELECT o.channel, COUNT(CASE WHEN p.status = 'FAILED' THEN 1 END) AS failed_count
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
GROUP BY o.channel
ORDER BY channel
