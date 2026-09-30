SELECT CASE WHEN amount >= 3700 THEN 'high' WHEN amount >= 1700 THEN 'mid' ELSE 'low' END AS amount_bucket, AVG(amount) AS avg_amount, ROUND((100 * SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refund
FROM transactions
GROUP BY amount_bucket
HAVING COUNT(*) >= 4
