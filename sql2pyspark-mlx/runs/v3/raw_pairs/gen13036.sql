SELECT a.id, a.account_type, a.status FROM accounts a WHERE a.balance > 77000 AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id IN (7, 14, 26, 28)) ORDER BY a.status
