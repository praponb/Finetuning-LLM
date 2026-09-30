SELECT
    c.city,
    c.signup_date,
    a.opened_date,
    a.account_type,
    a.balance
FROM customers c
LEFT JOIN accounts a ON a.customer_id = c.id
WHERE a.balance > 55000 AND a.customer_id IN (12, 28)
