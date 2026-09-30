SELECT store_name, city
FROM stores
WHERE city != 'New York'
UNION ALL
SELECT store_name, city
FROM stores
WHERE region IN ('East', 'South', 'North')
ORDER BY store_name;
