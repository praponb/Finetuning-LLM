SELECT *, LENGTH(city) AS city_length
FROM suppliers
WHERE city != 'London'
ORDER BY 2;
