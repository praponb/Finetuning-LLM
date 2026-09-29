SELECT status, ROUND(AVG(cost), 2) AS avg_cost, COUNT(cost) AS cost_count
FROM shipments
GROUP BY status
HAVING ROUND(AVG(cost), 2) >= 41
ORDER BY avg_cost, status DESC
