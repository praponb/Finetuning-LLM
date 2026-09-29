SELECT * FROM (SELECT status, account_type, balance FROM accounts WHERE status = 'FROZEN') PIVOT (SUM(balance) FOR account_type IN ('LOAN', 'CREDIT', 'SAVINGS'))
