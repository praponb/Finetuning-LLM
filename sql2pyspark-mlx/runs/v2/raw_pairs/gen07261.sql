SELECT
    id,
    txn_type,
    amount,
    SUM(amount) OVER (PARTITION BY txn_type) AS txn_type_sum_amount
FROM transactions;
