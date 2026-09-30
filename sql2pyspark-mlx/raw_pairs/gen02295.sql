SELECT o.status, o.channel, COUNT(s.cost) AS cost_count
FROM shipments s
JOIN orders o ON o.id = s.order_id
WHERE s.status NOT IN ('DELIVERED', 'LOST', 'IN_TRANSIT')
GROUP BY o.status, o.channel
ORDER BY o.status ASC, cost_count ASC, channel ASC
LIMIT 3
