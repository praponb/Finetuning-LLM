SELECT
    id,
    merchant,
    txn_time,
    amount,
    LAG(amount, 1, 0) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_amount
FROM transactions;
