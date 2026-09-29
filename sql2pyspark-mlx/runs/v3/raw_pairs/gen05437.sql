SELECT account_type, status, AVG(balance) AS avg_balance, SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_count FROM accounts GROUP BY account_type, status ORDER BY status, account_type
