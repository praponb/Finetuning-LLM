SELECT a.id, a.account_type FROM accounts a WHERE a.account_type <> 'SAVINGS' AND a.customer_id NOT IN (SELECT id FROM customers WHERE city IS NULL);
