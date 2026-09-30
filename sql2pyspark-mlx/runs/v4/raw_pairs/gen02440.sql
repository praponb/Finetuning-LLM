SELECT order_id, ROUND((100 * SUM(CASE WHEN status = 'LOST' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lost, AVG(cost) AS avg_cost
FROM shipments
WHERE status = 'DELIVERED'
GROUP BY order_id
ORDER BY avg_cost DESC
