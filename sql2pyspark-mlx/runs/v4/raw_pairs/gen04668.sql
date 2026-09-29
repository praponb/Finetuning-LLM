SELECT
    id,
    amount,
    is_flagged,
    txn_type,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE txn_time < '2023-05-29' AND merchant IS NOT NULL
ORDER BY id DESC
