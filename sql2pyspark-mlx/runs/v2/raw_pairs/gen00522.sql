SELECT
    a.status,
    a.account_type,
    c.is_active,
    ROUND(c.age * 1.15, 2) AS adjusted_age
FROM accounts a
RIGHT OUTER JOIN customers c ON a.customer_id = c.id
WHERE c.city = 'Bangkok'
