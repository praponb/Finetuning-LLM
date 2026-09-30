SELECT CASE WHEN cost >= 106 THEN 'high' WHEN cost >= 19 THEN 'mid' ELSE 'low' END AS cost_bucket, SUM(cost) AS total_cost, RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk
FROM shipments
WHERE order_id IS NOT NULL
GROUP BY CASE WHEN cost >= 106 THEN 'high' WHEN cost >= 19 THEN 'mid' ELSE 'low' END
ORDER BY 3 DESC
