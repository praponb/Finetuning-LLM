SELECT a.id, a.customer_id, a.account_type FROM accounts a WHERE a.balance > 30000 AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type = 'REFUND') ORDER BY account_type
