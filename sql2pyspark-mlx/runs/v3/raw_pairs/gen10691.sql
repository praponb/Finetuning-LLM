SELECT YEAR(opened_date) AS opened_year, SUM(CASE WHEN status = 'OPEN' THEN balance ELSE 0 END) AS open_balance, COUNT(DISTINCT balance) AS unique_account_types
FROM accounts
GROUP BY YEAR(opened_date)
HAVING COUNT(DISTINCT balance) > 2
ORDER BY open_balance DESC
