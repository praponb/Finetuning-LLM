SELECT
    id,
    country,
    age,
    city,
    signup_date,
    credit_limit,
    AVG(age) OVER (PARTITION BY country) AS country_avg_age,
    SUM(credit_limit) OVER (PARTITION BY city ORDER BY signup_date, id) AS running_credit_limit
FROM customers
WHERE YEAR(signup_date) >= 2023
ORDER BY country_avg_age DESC;
