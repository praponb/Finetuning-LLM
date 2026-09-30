SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    amount,
    LAG(amount, 1) OVER (PARTITION BY account_id, txn_type ORDER BY txn_time, id) AS prev_amount
FROM transactions
ORDER BY txn_time DESC;
