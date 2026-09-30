SELECT region, city, SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END) AS chiang_mai_count, MIN(sqft) AS min_sqft FROM stores GROUP BY region, city
