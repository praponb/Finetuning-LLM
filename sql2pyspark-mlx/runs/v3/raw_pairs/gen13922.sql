SELECT id, city, SUBSTRING(country, 1, 3) AS country_prefix
FROM suppliers
WHERE rating BETWEEN 2.2 AND 3.1;
