SELECT QUARTER(opened_date) AS opened_quarter, AVG(balance) AS avg_balance, ROUND(STDDEV(balance), 2) AS stddev_balance FROM accounts GROUP BY 1 ORDER BY opened_quarter
