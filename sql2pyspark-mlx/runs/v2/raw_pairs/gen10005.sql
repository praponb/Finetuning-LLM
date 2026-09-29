SELECT
    id,
    amount,
    txn_type,
    merchant,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
ORDER BY amount, id
LIMIT 50;
