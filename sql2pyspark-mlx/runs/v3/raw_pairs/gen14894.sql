SELECT a.account_type, a.opened_date, a.status, c.signup_date, c.segment FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id;
