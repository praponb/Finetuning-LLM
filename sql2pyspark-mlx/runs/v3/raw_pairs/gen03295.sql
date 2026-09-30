SELECT o.status, ROUND((100 * SUM(CASE WHEN s.status = 'LOST' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_lost, SUM(s.cost) total_cost
FROM shipments s
JOIN orders o ON s.order_id = o.id
JOIN customers c ON c.id = o.customer_id
GROUP BY o.status
HAVING ROUND((100 * SUM(CASE WHEN s.status = 'LOST' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 61;
