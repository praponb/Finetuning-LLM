SELECT CASE WHEN amount >= 3300 THEN 'high' WHEN amount >= 1900 THEN 'mid' ELSE 'low' END AS amount_bucket, ROUND(AVG(amount), 2) AS avg_amount, MAX(amount) - MIN(amount) AS amount_range
FROM transactions
WHERE amount <= 2600 AND amount >= 1100
GROUP BY CASE WHEN amount >= 3300 THEN 'high' WHEN amount >= 1900 THEN 'mid' ELSE 'low' END
HAVING COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) < 4
ORDER BY amount_bucket;
