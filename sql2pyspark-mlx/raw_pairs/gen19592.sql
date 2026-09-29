SELECT status, account_type, COUNT(DISTINCT balance) unique_statuss
FROM accounts
GROUP BY status, account_type
ORDER BY unique_statuss;
