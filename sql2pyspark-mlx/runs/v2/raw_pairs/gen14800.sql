SELECT status, MAX(balance) - MIN(balance) AS balance_range, AVG(balance) AS avg_balance FROM accounts GROUP BY status;
