SELECT status, MAX(cost) AS max_cost, COUNT(DISTINCT cost) AS unique_statuss
FROM shipments
WHERE status IN ('RETURNED', 'DELIVERED') AND delivered_date > '2023-07-01'
GROUP BY status
HAVING MAX(cost) < 30
ORDER BY status
