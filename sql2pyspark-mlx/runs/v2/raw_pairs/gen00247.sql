SELECT opened_date, CONCAT_WS('-', city, store_name) AS city_store_name_key FROM stores WHERE store_name LIKE '%Kio%'
