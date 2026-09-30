SELECT CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END AS txn_cohort, COUNT(DISTINCT amount) AS unique_txn_types, SUM(amount) AS total_amount
FROM transactions
GROUP BY txn_cohort
ORDER BY total_amount DESC, txn_cohort DESC
LIMIT 1
