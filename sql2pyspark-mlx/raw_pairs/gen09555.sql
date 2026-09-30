SELECT
    id,
    merchant,
    txn_time,
    amount,
    SUM(amount) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS running_amount
FROM transactions;
