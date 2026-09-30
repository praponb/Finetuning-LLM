SELECT is_flagged, DATE_TRUNC('MONTH', txn_time) AS txn_month, COUNT(DISTINCT amount) AS unique_accounts
FROM transactions
WHERE TO_DATE(txn_time) < '2024-06-01'
GROUP BY is_flagged, DATE_TRUNC('MONTH', txn_time)
ORDER BY unique_accounts, txn_month
