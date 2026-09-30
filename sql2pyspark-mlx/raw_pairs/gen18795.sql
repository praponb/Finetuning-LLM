SELECT
    id,
    segment,
    signup_date,
    credit_limit,
    country,
    email,
    credit_limit - LAG(credit_limit, 1) OVER (PARTITION BY segment ORDER BY signup_date, id) AS credit_limit_change,
    LAST_VALUE(email) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_email
FROM customers;
