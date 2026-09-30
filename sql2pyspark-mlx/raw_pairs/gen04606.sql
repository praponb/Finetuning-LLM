SELECT a.account_type, a.status, c.age, c.signup_date, c.country FROM accounts a JOIN customers c ON a.customer_id = c.id ORDER BY c.signup_date
