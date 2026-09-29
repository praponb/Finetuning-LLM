SELECT
    id,
    txn_type,
    amount,
    MAX(amount) OVER (PARTITION BY txn_type) AS txn_type_max_amount
FROM transactions
WHERE NOT is_flagged
ORDER BY txn_type_max_amount ASC;
