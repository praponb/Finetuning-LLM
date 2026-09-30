SELECT DISTINCT region
FROM stores
WHERE LOWER(TRIM(store_name)) = 'old town'
