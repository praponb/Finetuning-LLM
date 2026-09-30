SELECT o.channel, COUNT(s.cost) AS cost_count, SUM(s.cost) AS total_cost
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
GROUP BY o.channel
ORDER BY total_cost DESC
