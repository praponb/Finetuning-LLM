SELECT
    a.id,
    a.account_type,
    a.customer_id,
    c.segment,
    c.credit_limit
FROM accounts a
JOIN customers c ON a.customer_id = c.id
