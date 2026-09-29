SELECT a.id, a.balance FROM accounts a WHERE a.balance > 34000 AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type = 'DEBIT')
