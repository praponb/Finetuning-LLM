SELECT * FROM (SELECT account_type, status, ROUND(AVG(balance), 2) avg_balance FROM accounts GROUP BY account_type, status) x WHERE avg_balance >= 74 ORDER BY account_type DESC
