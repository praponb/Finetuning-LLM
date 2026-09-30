SELECT a.id, a.account_type, a.balance FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE email IS NOT NULL);
