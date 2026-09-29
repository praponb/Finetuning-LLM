SELECT id, opened_date, city, store_name, UPPER(city) AS city_upper, UPPER(TRIM(store_name)) AS store_name_normalized FROM stores
