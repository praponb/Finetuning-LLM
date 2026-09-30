SELECT CASE WHEN amount >= 3300 THEN 'high' WHEN amount >= 1900 THEN 'mid' ELSE 'low' END AS amount_bucket, ROUND((100 * SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shell, ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE is_flagged = FALSE AND amount < 1900
GROUP BY amount_bucket
ORDER BY pct_shell DESC, amount_bucket
LIMIT 20;
