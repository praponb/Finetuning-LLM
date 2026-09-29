SELECT * FROM (SELECT account_type, status, balance FROM accounts WHERE status IN ('OPEN', 'CLOSED', 'FROZEN')) PIVOT (COUNT(balance) FOR status IN ('FROZEN', 'CLOSED'))
