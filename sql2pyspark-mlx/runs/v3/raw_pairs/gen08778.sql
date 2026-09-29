SELECT
    id,
    txn_type,
    merchant,
    account_id,
    DAY(txn_time) AS txn_time_day,
    COALESCE(merchant, 'unassigned') AS merchant_filled
FROM transactions
WHERE is_flagged = TRUE
ORDER BY account_id;
