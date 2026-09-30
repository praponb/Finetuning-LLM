SELECT a.id, a.opened_date FROM accounts a WHERE a.account_type = 'LOAN' AND a.customer_id NOT IN (SELECT id FROM customers WHERE age IS NOT NULL) ORDER BY id DESC, a.opened_date LIMIT 10
