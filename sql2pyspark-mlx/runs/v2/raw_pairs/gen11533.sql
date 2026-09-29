SELECT
    id,
    country,
    signup_date,
    credit_limit,
    LEAD(credit_limit, 2) OVER (PARTITION BY country ORDER BY signup_date, id) AS next_credit_limit
FROM customers
ORDER BY signup_date DESC, country DESC;
