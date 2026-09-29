SELECT id, city, sqft, opened_date, TRIM(region) AS clean_region, UPPER(city) AS city_upper FROM stores WHERE MONTH(opened_date) IN (2, 5, 9)
