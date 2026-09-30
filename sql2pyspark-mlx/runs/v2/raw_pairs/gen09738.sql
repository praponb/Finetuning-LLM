SELECT
    id,
    is_flagged,
    INITCAP(TRIM(merchant)) AS merchant_title,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE account_id IN (19, 27) AND amount >= 2100
ORDER BY is_flagged_flag DESC
