SELECT status, COUNT(DISTINCT balance) unique_statuss, ROUND(AVG(balance), 2) avg_balance
FROM accounts
WHERE balance IS NOT NULL
GROUP BY status
ORDER BY unique_statuss DESC
