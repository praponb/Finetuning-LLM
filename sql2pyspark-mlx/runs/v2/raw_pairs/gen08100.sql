SELECT status, AVG(cost) AS avg_cost, SUM(cost) AS total_cost
FROM shipments
WHERE status IN ('DELIVERED', 'LOST', 'RETURNED') AND order_id IN (3, 19, 32, 36) AND shipped_date < '2023-08-01'
GROUP BY status
ORDER BY status DESC
