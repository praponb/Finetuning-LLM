SELECT a.balance, a.customer_id, a.account_type, t.account_id FROM accounts a JOIN transactions t ON t.account_id = a.id WHERE a.customer_id IS NULL ORDER BY a.account_type DESC
