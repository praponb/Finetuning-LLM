SELECT
    a.id,
    a.balance,
    c.country,
    c.credit_limit
FROM accounts a
LEFT OUTER JOIN customers c ON c.id = a.customer_id
WHERE c.country = 'SG'
