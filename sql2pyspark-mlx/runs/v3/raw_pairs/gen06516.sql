SELECT a.id, a.customer_id, a.status FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.amount IS NOT NULL)
