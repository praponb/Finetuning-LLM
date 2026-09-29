SELECT id, region, city, sqft, AVG(sqft) OVER (PARTITION BY region, city) AS region_avg_sqft FROM stores WHERE city = 'London' ORDER BY 2, sqft DESC;
