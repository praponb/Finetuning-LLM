SELECT a.id, a.account_type, a.customer_id FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) ORDER BY customer_id DESC
