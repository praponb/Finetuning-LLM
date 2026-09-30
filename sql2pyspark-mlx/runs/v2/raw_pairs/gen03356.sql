SELECT carrier, SUM(cost) AS total_cost, DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS cost_rank
FROM shipments
GROUP BY carrier;
