SELECT
    id,
    signup_date,
    city,
    credit_limit,
    SUM(credit_limit) OVER (PARTITION BY YEAR(signup_date), city ORDER BY signup_date, id) AS running_credit_limit
FROM customers;
