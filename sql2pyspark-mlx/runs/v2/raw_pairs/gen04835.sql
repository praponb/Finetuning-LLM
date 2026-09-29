SELECT a.id, a.balance, a.customer_id FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.is_flagged = TRUE) ORDER BY customer_id DESC, balance DESC
