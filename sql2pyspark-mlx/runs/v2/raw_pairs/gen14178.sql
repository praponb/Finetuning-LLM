SELECT
    id,
    country,
    signup_date,
    age,
    LEAD(age, 2) OVER (PARTITION BY country ORDER BY signup_date, id) AS next_age
FROM customers
