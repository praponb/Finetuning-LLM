SELECT store_name
FROM stores
WHERE store_name IS NOT NULL
INTERSECT
SELECT store_name
FROM stores
WHERE store_name NOT LIKE 'M%'
ORDER BY store_name DESC
