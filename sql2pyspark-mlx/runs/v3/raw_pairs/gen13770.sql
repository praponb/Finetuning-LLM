SELECT
    id,
    segment,
    signup_date,
    age,
    LEAD(age, 1) OVER (PARTITION BY segment ORDER BY signup_date, id) AS next_age
FROM customers
