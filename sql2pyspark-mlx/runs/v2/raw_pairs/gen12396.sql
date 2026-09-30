SELECT a.id, a.account_type FROM accounts a WHERE a.balance < 45000 AND a.customer_id IN (SELECT id FROM customers WHERE city = 'New York') ORDER BY id DESC
