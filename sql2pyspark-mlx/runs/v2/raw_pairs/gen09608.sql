SELECT
    id,
    country,
    age,
    MAX(age) OVER (PARTITION BY country) AS country_max_age
FROM customers
WHERE name LIKE 'Da%'
