SELECT a.id, a.status, a.customer_id, a.balance FROM accounts a WHERE a.account_type IN ('LOAN', 'SAVINGS', 'CREDIT') AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id)
