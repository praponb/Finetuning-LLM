SELECT * FROM (SELECT account_type, status, balance FROM accounts)
PIVOT (COUNT(balance) FOR status IN ('CLOSED', 'OPEN', 'FROZEN'))
ORDER BY account_type
