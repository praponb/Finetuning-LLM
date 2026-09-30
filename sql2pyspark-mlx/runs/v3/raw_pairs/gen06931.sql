SELECT city, region, MIN(sqft) AS min_sqft, COUNT(DISTINCT sqft) AS unique_citys FROM stores GROUP BY city, region HAVING MIN(sqft) > 15200 ORDER BY min_sqft, unique_citys DESC
