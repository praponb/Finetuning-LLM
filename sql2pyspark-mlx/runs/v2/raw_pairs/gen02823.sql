SELECT a.id, a.account_type, a.status FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE YEAR(signup_date) = 2024);
