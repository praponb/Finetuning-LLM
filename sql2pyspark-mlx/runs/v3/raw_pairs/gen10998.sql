SELECT
    id,
    merchant,
    amount,
    account_id,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE amount BETWEEN 1400 AND 3500
