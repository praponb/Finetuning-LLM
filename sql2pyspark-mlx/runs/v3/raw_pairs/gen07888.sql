SELECT status, SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) AS frozen_balance, MAX(balance) AS max_balance, ROUND(AVG(balance), 2) AS avg_balance FROM accounts GROUP BY status
