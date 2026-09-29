SELECT id, account_id, txn_time, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS txn_time_row FROM transactions WHERE merchant IN ('7-Eleven', 'Amazon', 'Lazada');
