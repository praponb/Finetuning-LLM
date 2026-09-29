SELECT
    id,
    txn_time,
    amount,
    merchant,
    amount - LAG(amount, 1) OVER (ORDER BY txn_time, id) AS amount_change,
    amount - LAG(amount, 1, 0) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS amount_change_2
FROM transactions
WHERE HOUR(txn_time) BETWEEN 10 AND 13
ORDER BY txn_time DESC;
