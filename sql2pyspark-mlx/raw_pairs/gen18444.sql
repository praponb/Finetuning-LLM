SELECT a.id, a.balance FROM accounts a WHERE a.account_type = 'LOAN' AND a.customer_id IN (SELECT id FROM customers WHERE country = 'JP');
