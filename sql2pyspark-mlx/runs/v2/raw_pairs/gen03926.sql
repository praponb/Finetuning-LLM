SELECT status, SUM(balance) AS total_balance, SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END) AS checking_count FROM accounts GROUP BY status HAVING AVG(balance) < 39000
