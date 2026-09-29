SELECT id, sqft, TRIM(region) AS clean_region, CONCAT_WS(',', region, city) AS region_city_key FROM stores WHERE (store_name NOT LIKE 'Ri%' AND sqft = 6808) OR region = 'Central'
