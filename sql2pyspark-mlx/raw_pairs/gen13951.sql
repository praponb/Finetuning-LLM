SELECT MONTH(opened_date) AS opened_month, MIN(balance) AS min_balance FROM accounts WHERE opened_date < '2025-10-01' GROUP BY MONTH(opened_date) ORDER BY min_balance DESC;
