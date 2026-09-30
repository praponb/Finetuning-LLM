SELECT id, city, sqft, AVG(sqft) OVER (PARTITION BY city) AS city_avg_sqft FROM stores WHERE sqft BETWEEN 11840 AND 14115;
