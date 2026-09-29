SELECT
    sqft,
    city,
    store_name,
    CONCAT_WS('-', store_name, city) AS store_name_city_key,
    CONCAT(region, ', ', store_name) AS region_store_name
FROM stores
WHERE store_name IS NOT NULL
ORDER BY sqft DESC, region_store_name;
