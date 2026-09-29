SELECT status, MIN(balance) AS min_balance
FROM accounts
GROUP BY status;
