SELECT
    id,
    city,
    segment,
    signup_date,
    credit_limit,
    age,
    LEAD(credit_limit, 1) OVER (PARTITION BY city, segment ORDER BY signup_date, id) AS next_credit_limit,
    LAST_VALUE(signup_date) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_signup_date
FROM customers
WHERE credit_limit < 5200
ORDER BY next_credit_limit DESC;
