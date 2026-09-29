SELECT CASE WHEN cost >= 97 THEN 'high' WHEN cost >= 42 THEN 'mid' ELSE 'low' END AS cost_bucket, SUM(cost) AS total_cost, SUM(SUM(cost)) OVER (ORDER BY CASE WHEN cost >= 97 THEN 'high' WHEN cost >= 42 THEN 'mid' ELSE 'low' END) AS cumulative_cost
FROM shipments
GROUP BY CASE WHEN cost >= 97 THEN 'high' WHEN cost >= 42 THEN 'mid' ELSE 'low' END
ORDER BY cost_bucket DESC
