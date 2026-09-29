SELECT
    id,
    city,
    signup_date,
    age,
    country,
    credit_limit,
    SUM(age) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age,
    ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country), 3) AS credit_limit_share
FROM customers
ORDER BY city;
