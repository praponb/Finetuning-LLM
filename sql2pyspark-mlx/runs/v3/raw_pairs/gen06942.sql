SELECT
    store_name,
    city,
    sqft,
    INITCAP(TRIM(city)) city_title,
    CONCAT_WS(',', city, region) city_region_key
FROM stores
WHERE city IN ('Bangkok', 'Berlin', 'Singapore') AND opened_date > '2024-04-01'
