SELECT
    id,
    txn_type,
    amount,
    MIN(amount) OVER (PARTITION BY txn_type) AS txn_type_min_amount
FROM transactions
ORDER BY amount DESC, txn_type;
