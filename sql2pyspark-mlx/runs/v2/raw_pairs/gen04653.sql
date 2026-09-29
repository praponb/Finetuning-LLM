SELECT id, opened_date, sqft, region, SUBSTRING(store_name, 1, 4) AS store_name_prefix FROM stores WHERE city = 'Berlin' ORDER BY 2, sqft, id LIMIT 3;
