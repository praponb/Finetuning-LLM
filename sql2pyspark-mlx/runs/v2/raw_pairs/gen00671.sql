SELECT region, city, MIN(sqft) AS min_sqft
FROM stores
GROUP BY region, city
