SELECT id, city, sqft, opened_date, MAX(sqft) OVER (PARTITION BY city) city_max_sqft, NTILE(2) OVER (PARTITION BY MONTH(opened_date) ORDER BY sqft DESC, id) quartile FROM stores ORDER BY id DESC;
