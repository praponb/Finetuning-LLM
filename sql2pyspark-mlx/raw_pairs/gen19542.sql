SELECT a.id, a.account_type, a.customer_id, a.status FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE email IS NOT NULL)
