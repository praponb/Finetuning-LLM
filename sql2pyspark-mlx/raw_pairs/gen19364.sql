SELECT
    a.id,
    a.account_type,
    a.balance,
    a.status,
    c.is_active,
    c.age
FROM accounts a
JOIN customers c ON a.customer_id = c.id
WHERE c.country = 'UK'
