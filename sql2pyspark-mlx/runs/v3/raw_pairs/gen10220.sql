SELECT status, account_type, stddev_balance FROM (SELECT status, account_type, ROUND(STDDEV(balance), 2) AS stddev_balance FROM accounts GROUP BY status, account_type) AS t WHERE stddev_balance < 57;
