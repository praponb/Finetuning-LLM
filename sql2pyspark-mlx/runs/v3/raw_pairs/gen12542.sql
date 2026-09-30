SELECT a.id, a.account_type, a.balance, a.customer_id FROM accounts a WHERE a.balance = (SELECT MIN(a2.balance) FROM accounts a2 WHERE a2.account_type = a.account_type);
