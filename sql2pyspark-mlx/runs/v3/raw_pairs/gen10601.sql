SELECT city, LOWER(country) AS country_lower
FROM suppliers
WHERE city IS NOT NULL AND city IS NOT NULL
ORDER BY country_lower DESC, city DESC;
