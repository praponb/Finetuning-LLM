SELECT account_type, COUNT(balance) AS balance_count, MAX(balance) AS max_balance
FROM accounts
GROUP BY account_type;
