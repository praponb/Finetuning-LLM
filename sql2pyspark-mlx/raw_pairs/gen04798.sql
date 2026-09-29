SELECT * FROM (SELECT account_type, status, balance FROM accounts WHERE YEAR(opened_date) = 2023)
PIVOT (SUM(balance) FOR status IN ('CLOSED', 'OPEN', 'FROZEN'));
