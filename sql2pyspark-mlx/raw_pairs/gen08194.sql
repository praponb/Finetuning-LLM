SELECT id, account_id, txn_type FROM transactions WHERE txn_time < '2024-08-01' UNION SELECT id, account_id, txn_type FROM transactions WHERE is_flagged;
