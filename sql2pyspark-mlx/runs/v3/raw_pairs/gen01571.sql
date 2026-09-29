SELECT a.id, a.customer_id FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE email LIKE '%@yahoo.com') ORDER BY a.customer_id DESC, a.id DESC LIMIT 1
