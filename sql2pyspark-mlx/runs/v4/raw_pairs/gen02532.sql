SELECT store_name, region, city, CONCAT_WS('/', city, region) AS city_region_key FROM stores WHERE sqft >= 5179 ORDER BY city_region_key;
