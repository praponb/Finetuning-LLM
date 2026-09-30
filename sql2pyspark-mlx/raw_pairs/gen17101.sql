SELECT a.id, a.opened_date, a.status, c.credit_limit, c.age, c.signup_date FROM accounts a JOIN customers c ON c.id = a.customer_id WHERE c.signup_date > '2025-02-01' OR a.balance > 17000
