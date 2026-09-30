SELECT id, store_name, TRIM(city) AS clean_city
FROM stores
WHERE store_name LIKE 'Ai%' AND region <> 'North';
