SELECT * FROM (SELECT account_type, status, balance FROM accounts)
PIVOT (MAX(balance) FOR status IN ('OPEN', 'CLOSED', 'FROZEN'));
