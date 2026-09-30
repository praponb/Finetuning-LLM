SELECT region, SUM(CASE WHEN region = 'Central' THEN sqft ELSE 0 END) AS central_sqft, ROUND(AVG(sqft), 2) AS avg_sqft
FROM stores
GROUP BY region
HAVING central_sqft > 53;
