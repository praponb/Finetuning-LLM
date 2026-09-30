SELECT account_type, status, ROUND(STDDEV(balance), 2) AS stddev_balance FROM accounts WHERE status = 'OPEN' GROUP BY 1, 2 ORDER BY stddev_balance, account_type, status LIMIT 20;
