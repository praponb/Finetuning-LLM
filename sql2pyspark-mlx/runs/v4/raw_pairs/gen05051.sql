SELECT
    CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END AS txn_cohort,
    txn_type,
    ROUND((100 * SUM(CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_grab,
    COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) AS amazon_count
FROM transactions
GROUP BY 1, 2
ORDER BY pct_grab;
