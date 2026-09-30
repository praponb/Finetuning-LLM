SELECT sqft, CONCAT_WS('-', city, region) AS city_region_key FROM stores WHERE city NOT IN ('Bangkok', 'New York') OR store_name IS NOT NULL ORDER BY city_region_key DESC, sqft
