SELECT account_id, DATE_TRUNC('MONTH', txn_time) AS txn_month, MAX(txn_time) AS last_txn_time
FROM transactions
GROUP BY 1, 2
ORDER BY account_id DESC, txn_month
LIMIT 3;
