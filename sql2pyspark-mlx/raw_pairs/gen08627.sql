SELECT
    merchant,
    COUNT(DISTINCT amount) AS unique_accounts,
    MIN(amount) AS min_amount,
    ROUND((100 * SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
FROM transactions
WHERE NOT is_flagged
GROUP BY merchant
