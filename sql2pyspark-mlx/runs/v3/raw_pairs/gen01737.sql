SELECT status, account_type, MAX(balance) - MIN(balance) AS balance_range
FROM accounts
WHERE account_type = 'CREDIT' AND account_type = 'CREDIT'
GROUP BY status, account_type
HAVING MAX(balance) - MIN(balance) < 74
ORDER BY balance_range, status;
