SELECT * FROM (SELECT YEAR(opened_date) AS opened_year, status, balance FROM accounts)
PIVOT (SUM(balance) FOR status IN ('OPEN', 'FROZEN'))
ORDER BY opened_year;
