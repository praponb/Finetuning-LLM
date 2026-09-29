SELECT name, INITCAP(TRIM(city)) AS city_title
FROM customers
WHERE city = 'London'
ORDER BY city_title
