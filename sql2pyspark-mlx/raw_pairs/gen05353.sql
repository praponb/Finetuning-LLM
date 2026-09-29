SELECT
    t.account_id,
    t.merchant,
    a.account_type,
    a.balance,
    c.country,
    c.age,
    DATE_TRUNC('MONTH', c.signup_date) AS signup_date_period
FROM transactions t
LEFT OUTER JOIN accounts a ON a.id = t.account_id
LEFT OUTER JOIN customers c ON c.id = a.customer_id
ORDER BY t.account_id DESC
