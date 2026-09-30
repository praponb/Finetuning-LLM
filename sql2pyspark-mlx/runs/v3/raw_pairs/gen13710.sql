SELECT merchant, COUNT(*) AS cnt
FROM transactions
GROUP BY merchant
ORDER BY cnt;
