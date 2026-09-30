SELECT
    id,
    country,
    segment,
    signup_date,
    age,
    LEAD(age, 1) OVER (PARTITION BY country, segment ORDER BY signup_date, id) AS next_age
FROM customers;
