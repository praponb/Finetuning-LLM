SELECT a.id, a.account_type, c.country
FROM accounts a
LEFT OUTER JOIN customers c ON a.customer_id = c.id
WHERE c.signup_date BETWEEN '2023-07-01' AND '2023-08-01'
