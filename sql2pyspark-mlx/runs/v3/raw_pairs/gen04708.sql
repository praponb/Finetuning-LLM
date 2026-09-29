SELECT
    id,
    merchant,
    account_id,
    txn_type
FROM transactions
WHERE is_flagged AND HOUR(txn_time) BETWEEN 7 AND 12 AND txn_time >= '2025-09-24'
