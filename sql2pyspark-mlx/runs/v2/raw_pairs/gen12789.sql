SELECT city, MAX(sqft) AS max_sqft FROM stores WHERE (region = 'Central' AND sqft != 13004) OR sqft IS NOT NULL GROUP BY city HAVING max_sqft < 10200 ORDER BY city;
