SELECT o.status, SUM(s.cost) total_cost, MIN(s.cost) min_cost
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
GROUP BY o.status
ORDER BY total_cost, status
LIMIT 5;
