SELECT
    id,
    txn_type,
    txn_time,
    amount,
    LAST_VALUE(amount) OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amount
FROM transactions
