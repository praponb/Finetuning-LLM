SELECT account_type, ROUND(STDDEV(balance), 2) AS stddev_balance, MIN(balance) AS min_balance
FROM accounts
WHERE account_type = 'CREDIT'
GROUP BY account_type
