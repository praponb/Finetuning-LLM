SELECT
    id,
    city,
    age,
    MIN(age) OVER (PARTITION BY city) city_min_age
FROM customers
WHERE signup_date <= '2023-03-01'
ORDER BY city_min_age DESC
