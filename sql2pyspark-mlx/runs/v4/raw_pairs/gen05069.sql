SELECT account_type, status, AVG(balance) avg_balance, MIN(balance) min_balance FROM accounts GROUP BY account_type, status ORDER BY account_type;
