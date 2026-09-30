SELECT status, COUNT(DISTINCT balance) AS unique_account_types
FROM accounts
GROUP BY status
ORDER BY status;
