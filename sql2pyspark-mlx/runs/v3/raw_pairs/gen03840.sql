SELECT
    is_flagged,
    txn_time,
    amount,
    SUBSTRING(txn_type, 1, 4) txn_type_prefix
FROM transactions
WHERE is_flagged = FALSE;
