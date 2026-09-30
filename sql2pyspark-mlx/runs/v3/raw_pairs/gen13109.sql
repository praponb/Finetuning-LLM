SELECT
    id,
    country,
    signup_date,
    age,
    SUM(age) OVER (PARTITION BY country ORDER BY signup_date, id) running_age
FROM customers
