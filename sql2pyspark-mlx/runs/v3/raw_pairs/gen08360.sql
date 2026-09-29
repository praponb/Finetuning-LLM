SELECT
    id,
    merchant,
    amount,
    txn_time,
    FIRST_VALUE(txn_time) OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS first_txn_time
FROM transactions
WHERE account_id IN (5, 26)
ORDER BY id DESC
