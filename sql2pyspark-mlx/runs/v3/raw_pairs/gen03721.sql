SELECT a.id, a.account_type FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_time < '2025-10-01') ORDER BY id, account_type LIMIT 5;
