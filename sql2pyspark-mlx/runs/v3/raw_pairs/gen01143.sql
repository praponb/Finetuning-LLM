SELECT a.id, a.status, a.customer_id, a.account_type FROM accounts a WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) ORDER BY a.status, customer_id DESC
