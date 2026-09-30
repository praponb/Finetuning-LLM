SELECT account_type, status, MAX(balance) - MIN(balance) AS balance_range
FROM accounts
GROUP BY account_type, status
HAVING MAX(balance) - MIN(balance) < 49
ORDER BY 2 DESC;
