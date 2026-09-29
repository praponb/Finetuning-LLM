SELECT account_type, MIN(opened_date) AS min_opened_date, SUM(balance) AS total_balance FROM accounts WHERE MONTH(opened_date) IN (4, 7, 12) GROUP BY account_type;
