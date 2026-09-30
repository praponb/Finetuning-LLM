SELECT city, region, MIN(sqft) AS min_sqft, MIN(opened_date) AS min_opened_date FROM stores WHERE LOWER(TRIM(store_name)) = 'riverside' GROUP BY city, region
