SELECT region, MIN(sqft) AS min_sqft
FROM stores
WHERE region = 'South'
GROUP BY region
ORDER BY region
LIMIT 5
