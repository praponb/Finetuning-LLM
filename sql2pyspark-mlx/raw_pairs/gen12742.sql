SELECT status, SUM(cost) AS total_cost, DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk
FROM shipments
GROUP BY status
