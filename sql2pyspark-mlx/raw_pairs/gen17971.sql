SELECT
    c.signup_date,
    a.account_type,
    a.opened_date,
    a.customer_id
FROM customers c
INNER JOIN accounts a ON a.customer_id = c.id
ORDER BY a.account_type, signup_date
