SELECT CASE WHEN cost >= 103 THEN 'high' WHEN cost >= 15 THEN 'mid' ELSE 'low' END AS cost_bucket, SUM(cost) AS total_cost, RANK() OVER (ORDER BY SUM(cost) DESC) AS cost_rank
FROM shipments
GROUP BY CASE WHEN cost >= 103 THEN 'high' WHEN cost >= 15 THEN 'mid' ELSE 'low' END
ORDER BY cost_rank ASC
