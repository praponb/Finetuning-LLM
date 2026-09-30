SELECT * FROM (SELECT status, account_type, balance FROM accounts WHERE account_type = 'SAVINGS')
PIVOT (MAX(balance) FOR account_type IN ('CHECKING', 'CREDIT'))
ORDER BY status
