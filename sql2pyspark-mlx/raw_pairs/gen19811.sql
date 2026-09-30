SELECT merchant, COUNT(DISTINCT amount) AS unique_merchants, ROUND((100 * SUM(CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lazada
FROM transactions
WHERE account_id = 4
GROUP BY merchant
HAVING COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END) < 5
ORDER BY unique_merchants ASC
