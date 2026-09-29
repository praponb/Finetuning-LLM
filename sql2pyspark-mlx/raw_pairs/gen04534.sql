SELECT a.opened_date, a.account_type, a.customer_id, c.email, c.age, c.signup_date FROM accounts a LEFT OUTER JOIN customers c ON c.id = a.customer_id ORDER BY email DESC;
