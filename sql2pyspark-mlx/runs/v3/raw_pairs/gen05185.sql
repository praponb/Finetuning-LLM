SELECT id, opened_date, LOWER(store_name) AS store_name_lower, CONCAT_WS('/', city, region) AS city_region_key FROM stores;
