SELECT region, MIN(sqft) AS min_sqft, ROUND(AVG(sqft), 2) AS avg_sqft
FROM stores
GROUP BY region
HAVING ROUND(AVG(sqft), 2) >= 41;
