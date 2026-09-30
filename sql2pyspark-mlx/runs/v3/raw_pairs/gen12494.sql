SELECT a.id, a.balance, a.account_type, a.status FROM accounts a WHERE a.customer_id IN (15, 26, 40) AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.amount >= 2000);
