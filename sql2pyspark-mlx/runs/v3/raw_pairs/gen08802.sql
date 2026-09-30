SELECT a.id, a.status, a.customer_id FROM accounts a WHERE a.status IS NULL AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) ORDER BY a.status, id LIMIT 5
