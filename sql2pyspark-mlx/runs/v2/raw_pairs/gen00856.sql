SELECT age, CONCAT_WS('/', country, segment) AS country_segment_key, LENGTH(city) AS city_length
FROM customers
WHERE city = 'Berlin'
ORDER BY city_length DESC, age DESC
