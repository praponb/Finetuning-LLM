SELECT region, CONCAT_WS('-', city, store_name) AS city_store_name_key, DAYOFWEEK(opened_date) AS opened_date_dayofweek FROM stores;
