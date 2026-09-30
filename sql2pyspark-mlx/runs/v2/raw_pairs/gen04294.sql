SELECT * FROM (SELECT MONTH(opened_date) AS opened_month, status, balance FROM accounts) PIVOT (AVG(balance) FOR status IN ('OPEN', 'FROZEN', 'CLOSED'));
