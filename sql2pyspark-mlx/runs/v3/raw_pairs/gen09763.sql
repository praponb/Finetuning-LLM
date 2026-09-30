SELECT
    id,
    region,
    store_name,
    city,
    CONCAT_WS(',', store_name, city) AS store_name_city_key,
    UPPER(region) AS region_upper
FROM stores
