SELECT merchant, DATE_TRUNC('MONTH', txn_time) AS txn_month, COUNT(*) AS total_count
FROM transactions
GROUP BY 1, 2
ORDER BY txn_month;
