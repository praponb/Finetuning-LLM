SELECT id, opened_date, city, sqft, LOWER(store_name) AS store_name_lower FROM stores ORDER BY sqft, 5 DESC;
