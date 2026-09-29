SELECT region, MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
WHERE sqft = 4611
GROUP BY region
HAVING sqft_range >= 25
ORDER BY sqft_range;
