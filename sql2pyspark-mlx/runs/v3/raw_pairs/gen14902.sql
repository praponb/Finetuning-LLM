SELECT
    id,
    country,
    signup_date,
    age,
    LEAD(age, 2) OVER (PARTITION BY country ORDER BY signup_date, id) AS next_age,
    RANK() OVER (PARTITION BY country ORDER BY age) AS age_rank
FROM customers
WHERE name LIKE 'N%'
