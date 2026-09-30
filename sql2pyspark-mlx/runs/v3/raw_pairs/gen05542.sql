SELECT * FROM (SELECT account_type, YEAR(opened_date) AS opened_year, MIN(balance) AS min_balance FROM accounts GROUP BY account_type, YEAR(opened_date)) AS sub WHERE min_balance < 35000
