SELECT account_type, MAX(balance) AS max_balance, AVG(balance) AS avg_balance FROM accounts GROUP BY account_type;
