SELECT
    id,
    txn_type,
    is_flagged,
    TRIM(txn_type) clean_txn_type
FROM transactions
WHERE NOT is_flagged
ORDER BY txn_type;
