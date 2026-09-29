SELECT
    a.id,
    a.status,
    a.account_type,
    c.name,
    c.email,
    c.credit_limit
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
