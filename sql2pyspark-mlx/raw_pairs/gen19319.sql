SELECT QUARTER(opened_date) AS opened_quarter, MAX(opened_date) AS latest_opened_date, MIN(balance) AS min_balance FROM accounts GROUP BY 1 HAVING min_balance >= 63000
