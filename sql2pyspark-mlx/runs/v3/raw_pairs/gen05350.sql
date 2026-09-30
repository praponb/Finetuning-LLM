SELECT a.id, a.balance, a.status, a.customer_id, c.is_active, c.signup_date, c.age FROM accounts a JOIN customers c ON a.customer_id = c.id;
