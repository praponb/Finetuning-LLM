SELECT * FROM (SELECT status, account_type, balance FROM accounts WHERE account_type = 'CHECKING') PIVOT (MAX(balance) FOR account_type IN ('CREDIT', 'SAVINGS', 'CHECKING', 'LOAN'))
