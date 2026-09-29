SELECT
    a.id,
    a.customer_id,
    a.status,
    c.credit_limit,
    c.is_active,
    c.signup_date
FROM accounts a
LEFT JOIN customers c ON c.id = a.customer_id
ORDER BY signup_date DESC, id
LIMIT 20;
