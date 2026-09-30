SELECT
    id,
    segment,
    signup_date,
    age,
    age - LAG(age, 1) OVER (PARTITION BY segment ORDER BY signup_date, id) AS age_change
FROM customers
