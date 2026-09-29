SELECT MONTH(opened_date) AS opened_month, YEAR(opened_date) AS opened_year, SUM(balance) AS total_balance
FROM accounts
GROUP BY ROLLUP(MONTH(opened_date), YEAR(opened_date));
