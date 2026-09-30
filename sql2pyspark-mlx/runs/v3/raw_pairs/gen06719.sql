SELECT
    t.id,
    t.merchant,
    t.account_id,
    a.account_type,
    c.credit_limit,
    c.signup_date
FROM transactions t
JOIN accounts a ON t.account_id = a.id
JOIN customers c ON c.id = a.customer_id
ORDER BY id DESC
