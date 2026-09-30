SELECT * FROM (SELECT account_type, status, balance FROM accounts)
PIVOT (COUNT(balance) FOR status IN ('OPEN', 'CLOSED'))
