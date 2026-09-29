SELECT account_id, COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count, MAX(txn_time) AS last_txn_time FROM transactions GROUP BY account_id;
