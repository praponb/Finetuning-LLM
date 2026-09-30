SELECT
    a.customer_id,
    c.is_active,
    c.credit_limit,
    c.signup_date
FROM accounts a
RIGHT JOIN customers c ON c.id = a.customer_id
