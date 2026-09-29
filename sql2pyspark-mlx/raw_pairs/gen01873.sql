SELECT
    id,
    merchant,
    account_id,
    txn_time,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE amount <= 2200
