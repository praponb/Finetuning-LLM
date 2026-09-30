SELECT
    id,
    city,
    signup_date,
    credit_limit,
    LEAD(credit_limit, 1) OVER (PARTITION BY city ORDER BY signup_date, id) AS next_credit_limit
FROM customers
WHERE name LIKE '%Chai%';
