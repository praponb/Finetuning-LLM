SELECT id, account_id, txn_time, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS txn_time_row FROM transactions;
