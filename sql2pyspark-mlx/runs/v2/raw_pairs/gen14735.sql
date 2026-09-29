SELECT
    id,
    merchant,
    INITCAP(TRIM(txn_type)) AS txn_type_title,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE amount BETWEEN 1600 AND 2800
ORDER BY id
LIMIT 10
