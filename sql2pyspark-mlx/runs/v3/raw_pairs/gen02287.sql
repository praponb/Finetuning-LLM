SELECT account_type, MIN(balance) AS min_balance
FROM accounts
WHERE balance <= 52000 AND status = 'FROZEN'
GROUP BY account_type
ORDER BY account_type;
