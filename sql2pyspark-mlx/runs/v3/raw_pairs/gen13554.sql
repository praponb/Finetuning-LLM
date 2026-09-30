SELECT
    id,
    city,
    age,
    MIN(age) OVER (PARTITION BY city) AS city_min_age
FROM customers
WHERE country IN ('SG', 'US', 'DE')
ORDER BY 2, city_min_age;
