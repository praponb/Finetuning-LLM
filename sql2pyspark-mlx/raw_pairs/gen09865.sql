SELECT id, sqft, city, SUBSTRING(city, 1, 4) AS city_prefix, LENGTH(region) AS region_length FROM stores ORDER BY region_length, sqft
