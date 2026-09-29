SELECT CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END AS txn_cohort, HOUR(txn_time) AS txn_hour, SUM(CASE WHEN merchant = 'Starbucks' THEN amount ELSE 0 END) AS starbucks_amount
FROM transactions
GROUP BY CUBE(CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END, HOUR(txn_time))
ORDER BY txn_cohort;
