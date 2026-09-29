SELECT
    id,
    country,
    credit_limit,
    age,
    FIRST_VALUE(age) OVER (PARTITION BY country ORDER BY credit_limit, id) AS first_age
FROM customers
ORDER BY age DESC, first_age DESC;
