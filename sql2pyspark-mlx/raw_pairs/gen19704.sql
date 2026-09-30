SELECT a.id, a.balance FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_time < '2023-04-01') ORDER BY a.balance, a.id
