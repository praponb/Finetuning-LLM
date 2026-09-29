SELECT
    id,
    region,
    sqft,
    LENGTH(region) AS region_length
FROM stores
WHERE LENGTH(store_name) >= 12
