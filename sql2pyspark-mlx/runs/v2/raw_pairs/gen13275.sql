SELECT CASE WHEN cost >= 640 THEN 'high' WHEN cost >= 270 THEN 'mid' ELSE 'low' END AS cost_bucket, MAX(cost) AS max_cost
FROM products
GROUP BY 1
HAVING MAX(cost) >= 460
ORDER BY max_cost
