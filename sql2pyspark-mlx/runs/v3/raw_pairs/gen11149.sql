SELECT
    id,
    txn_time,
    amount,
    is_flagged,
    HOUR(txn_time) AS txn_time_hour
FROM transactions
WHERE is_flagged = TRUE;
