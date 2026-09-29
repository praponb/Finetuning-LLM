SELECT
    id,
    txn_time,
    amount,
    txn_type,
    SUM(amount) OVER (ORDER BY txn_time, id) AS running_amount,
    COUNT(*) OVER (PARTITION BY txn_type) AS txn_type_count
FROM transactions
