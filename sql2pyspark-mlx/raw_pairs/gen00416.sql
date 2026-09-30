SELECT store_name
FROM stores
WHERE store_name LIKE 'O%'
UNION
SELECT store_name
FROM stores
WHERE region IN ('West', 'North', 'South')
ORDER BY store_name DESC;
