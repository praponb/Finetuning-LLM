SELECT
    id,
    region,
    store_name,
    sqft
FROM stores
WHERE city NOT IN ('Berlin', 'Singapore')
ORDER BY sqft DESC, store_name;
