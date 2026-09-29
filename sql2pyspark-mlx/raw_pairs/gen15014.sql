SELECT * FROM (SELECT MONTH(opened_date) AS opened_month, account_type, balance FROM accounts) PIVOT (AVG(balance) FOR account_type IN ('SAVINGS', 'LOAN'))
