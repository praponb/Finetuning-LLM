SELECT
    id,
    account_id,
    txn_type,
    merchant,
    CAST(txn_time AS DATE) AS txn_time_day
FROM transactions
WHERE txn_time >= '2024-03-23' AND (amount >= 3100 OR is_flagged)
ORDER BY txn_time_day DESC
