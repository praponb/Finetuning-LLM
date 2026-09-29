SELECT QUARTER(txn_time) AS txn_quarter, account_id, MAX(txn_time) AS last_txn_time
FROM transactions
GROUP BY QUARTER(txn_time), account_id
