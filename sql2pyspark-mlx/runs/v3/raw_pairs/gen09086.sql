SELECT
    sqft,
    city,
    region,
    LENGTH(store_name) AS store_name_length
FROM stores
WHERE region = 'West'
ORDER BY store_name_length DESC
