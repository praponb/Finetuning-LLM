SELECT *
FROM stores
WHERE store_name LIKE '%Riverside%'
ORDER BY store_name DESC, id
LIMIT 10
