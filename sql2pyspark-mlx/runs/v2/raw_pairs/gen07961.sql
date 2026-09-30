SELECT ROUND((100 * SUM(CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_grab
FROM transactions
ORDER BY 1 DESC;
