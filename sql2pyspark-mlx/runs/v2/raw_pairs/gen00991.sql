SELECT
    id,
    is_flagged,
    LENGTH(txn_type) txn_type_length,
    CAST(txn_time AS DATE) txn_time_day
FROM transactions
WHERE is_flagged = FALSE
ORDER BY id DESC
