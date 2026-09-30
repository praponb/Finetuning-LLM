SELECT id, region, sqft, city, MAX(sqft) OVER (PARTITION BY region) AS region_max_sqft, COUNT(*) OVER (PARTITION BY city) AS city_count FROM stores WHERE city = 'Singapore'
