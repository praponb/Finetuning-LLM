SELECT
    account_id,
    merchant,
    AVG(amount) AS avg_amount,
    MAX(txn_time) AS latest_txn_time
FROM transactions
GROUP BY account_id, merchant
HAVING AVG(amount) < 1000
ORDER BY merchant, account_id
LIMIT 10
