SELECT a.id, a.customer_id, a.status FROM accounts a WHERE a.balance < 40000 AND a.customer_id NOT IN (SELECT id FROM customers WHERE email LIKE '%@company.co');
