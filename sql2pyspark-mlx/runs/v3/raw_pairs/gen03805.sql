SELECT CASE WHEN amount >= 3100 THEN 'high' WHEN amount >= 900 THEN 'mid' ELSE 'low' END AS amount_bucket, CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END AS txn_cohort, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM transactions
WHERE merchant IN ('Shell', '7-Eleven')
GROUP BY 1, 2
