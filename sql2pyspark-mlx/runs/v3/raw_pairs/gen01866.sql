SELECT account_id, amount, TRIM(txn_type) AS clean_txn_type FROM transactions WHERE txn_type != 'FEE' AND txn_time >= '2025-08-01' AND amount <= 2600 ORDER BY clean_txn_type;
