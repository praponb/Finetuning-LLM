SELECT
    a.id,
    a.opened_date,
    c.country,
    c.segment,
    c.signup_date,
    ROUND(c.credit_limit / c.age, 2) credit_limit_per_age
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
ORDER BY a.opened_date;
