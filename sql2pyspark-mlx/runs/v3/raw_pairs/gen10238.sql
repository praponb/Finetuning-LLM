SELECT CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END AS txn_cohort, TO_DATE(txn_time) AS txn_day, COUNT(DISTINCT amount) AS unique_accounts
FROM transactions
GROUP BY txn_cohort, txn_day
ORDER BY unique_accounts DESC;
