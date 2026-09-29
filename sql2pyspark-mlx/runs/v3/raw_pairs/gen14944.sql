SELECT * FROM (SELECT status, account_type, balance FROM accounts WHERE balance > 53000)
PIVOT (SUM(balance) FOR account_type IN ('CREDIT', 'SAVINGS'))
ORDER BY status
