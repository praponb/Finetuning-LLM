SELECT region, ROUND(AVG(sqft), 2) AS avg_sqft, MAX(sqft) AS max_sqft
FROM stores
WHERE city IN ('New York', 'London')
GROUP BY region
HAVING SUM(CASE WHEN region = 'East' THEN sqft ELSE 0 END) < 46
ORDER BY max_sqft DESC, region ASC
LIMIT 1
