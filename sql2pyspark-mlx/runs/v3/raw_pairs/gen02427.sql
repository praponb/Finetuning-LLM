SELECT * FROM (SELECT account_type, status, balance FROM accounts)
PIVOT (SUM(balance) FOR status IN ('OPEN', 'FROZEN'));
