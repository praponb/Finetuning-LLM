SELECT account_type, MAX(balance) max_balance, MIN(balance) min_balance
FROM accounts
GROUP BY account_type
ORDER BY max_balance ASC;
