SELECT
    id,
    txn_type,
    txn_time,
    amount,
    LEAD(amount, 2, 0) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS next_amount
FROM transactions;
