SELECT account_id, COUNT(*) AS total_count, MIN(txn_time) AS min_txn_time
FROM transactions
GROUP BY account_id;
