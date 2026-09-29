SELECT account_type, MIN(balance) AS min_balance, COUNT(*) AS account_count FROM accounts GROUP BY account_type ORDER BY account_count
