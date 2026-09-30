SELECT status, COUNT(DISTINCT balance) AS unique_account_types
FROM accounts
GROUP BY status
HAVING COUNT(DISTINCT balance) >= 1;
