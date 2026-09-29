SELECT t.account_id, t.amount, a.opened_date, c.signup_date FROM transactions t JOIN accounts a ON a.id = t.account_id LEFT JOIN customers c ON a.customer_id = c.id WHERE t.is_flagged
