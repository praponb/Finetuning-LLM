SELECT a.id, a.status, c.signup_date
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
