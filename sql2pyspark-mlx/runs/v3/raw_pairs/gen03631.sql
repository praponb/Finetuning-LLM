SELECT id, city, sqft, region, AVG(sqft) OVER (PARTITION BY city) AS city_avg_sqft, COUNT(*) OVER (PARTITION BY region) AS region_count FROM stores ORDER BY city
