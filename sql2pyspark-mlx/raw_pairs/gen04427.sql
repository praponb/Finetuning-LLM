SELECT
    a.id,
    a.account_type,
    a.balance,
    c.email,
    c.age,
    c.is_active
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
