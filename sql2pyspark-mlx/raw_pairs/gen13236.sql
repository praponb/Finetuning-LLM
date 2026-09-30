SELECT YEAR(opened_date) AS opened_year, MIN(balance) AS min_balance
FROM accounts
GROUP BY 1
