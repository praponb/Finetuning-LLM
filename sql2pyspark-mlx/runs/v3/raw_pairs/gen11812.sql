SELECT account_type, SUM(balance) AS total_balance FROM accounts GROUP BY 1 HAVING total_balance < 361000 ORDER BY account_type DESC;
