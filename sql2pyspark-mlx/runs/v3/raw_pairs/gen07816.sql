SELECT
    city,
    region,
    sqft,
    COALESCE(store_name, 'unassigned') AS store_name_filled
FROM stores
WHERE LENGTH(store_name) < 9 AND (sqft = 8060 OR region != 'North')
ORDER BY store_name_filled, sqft DESC
