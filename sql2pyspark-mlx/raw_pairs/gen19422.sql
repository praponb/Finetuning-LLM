SELECT
    YEAR(txn_time) AS txn_year,
    merchant,
    MIN(amount) AS min_amount,
    COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count
FROM transactions
GROUP BY txn_year, merchant
ORDER BY 3 DESC;
