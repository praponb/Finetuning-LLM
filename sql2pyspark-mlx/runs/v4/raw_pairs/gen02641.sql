SELECT
    id,
    city,
    signup_date,
    RANK() OVER (PARTITION BY city ORDER BY signup_date) AS signup_date_rank
FROM customers
WHERE credit_limit BETWEEN 10800 AND 15700
ORDER BY city;
