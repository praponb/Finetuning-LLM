SELECT account_id FROM transactions WHERE account_id IN (18, 25, 37) UNION ALL SELECT account_id FROM transactions WHERE txn_time < '2024-12-31'
