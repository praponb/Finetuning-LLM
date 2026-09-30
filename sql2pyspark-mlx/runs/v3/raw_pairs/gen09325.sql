SELECT region, MIN(sqft) AS min_sqft
FROM stores
WHERE sqft <= 8111 AND store_name LIKE '%Old%'
GROUP BY region
ORDER BY region, min_sqft DESC
