SELECT id, store_name, city, TRIM(city) AS clean_city FROM stores WHERE region != 'South' ORDER BY store_name
