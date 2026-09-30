SELECT status, ROUND(AVG(balance), 2) AS avg_balance, COUNT(*) AS total_count FROM accounts GROUP BY status ORDER BY status;
