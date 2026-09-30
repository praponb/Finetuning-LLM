SELECT city, CONCAT_WS('-', city, region) AS city_region_key
FROM stores
WHERE store_name LIKE '%Air%';
