SELECT city, region, AVG(sqft) AS avg_sqft
FROM stores
GROUP BY city, region
HAVING AVG(sqft) < 16000
