SELECT a.id, a.account_type, a.balance FROM accounts a WHERE a.balance BETWEEN 28000 AND 52000 AND a.customer_id IN (SELECT id FROM customers WHERE credit_limit > 13700);
