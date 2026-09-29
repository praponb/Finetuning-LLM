SELECT store_name
FROM stores
WHERE sqft < 11893
UNION
SELECT store_name
FROM stores
WHERE region = 'Central'
