SELECT a.balance, a.status, c.signup_date FROM accounts a JOIN customers c ON a.customer_id = c.id WHERE c.age < 53 ORDER BY status DESC
