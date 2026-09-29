SELECT a.id, a.opened_date, a.account_type, c.signup_date FROM accounts a LEFT OUTER JOIN customers c ON a.customer_id = c.id WHERE a.balance > 45000;
