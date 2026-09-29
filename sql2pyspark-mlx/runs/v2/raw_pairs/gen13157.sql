SELECT status, AVG(balance) avg_balance, MIN(opened_date) min_opened_date, MAX(opened_date) latest_opened_date FROM accounts GROUP BY status ORDER BY latest_opened_date ASC
