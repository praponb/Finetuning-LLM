SELECT status, MIN(opened_date) AS min_opened_date, COUNT(balance) AS balance_count FROM accounts GROUP BY status;
