SELECT * FROM (SELECT MONTH(opened_date) AS opened_month, status, balance FROM accounts WHERE balance <= 39000) PIVOT (AVG(balance) FOR status IN ('FROZEN', 'CLOSED', 'OPEN')) ORDER BY opened_month
