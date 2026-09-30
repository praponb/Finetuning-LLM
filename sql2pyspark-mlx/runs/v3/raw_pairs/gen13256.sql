SELECT
    id,
    country,
    age,
    MIN(age) OVER (PARTITION BY country) AS country_min_age
FROM customers
WHERE name LIKE '%bob%'
ORDER BY country_min_age DESC;
