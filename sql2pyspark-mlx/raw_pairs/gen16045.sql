SELECT id, account_id, txn_time, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS rn FROM transactions WHERE txn_time < '2024-07-05'
