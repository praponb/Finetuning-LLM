SELECT
    id,
    txn_type,
    txn_time,
    amount,
    amount - LAG(amount, 2) OVER (PARTITION BY txn_type ORDER BY txn_time, id) amount_change
FROM transactions
