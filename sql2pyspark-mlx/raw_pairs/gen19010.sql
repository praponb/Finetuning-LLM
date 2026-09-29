SELECT
    a.balance,
    c.credit_limit,
    c.signup_date,
    c.age
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
ORDER BY age, c.signup_date
