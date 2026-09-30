SELECT * FROM (SELECT status, account_type, balance FROM accounts) PIVOT (AVG(balance) FOR account_type IN ('CREDIT', 'LOAN', 'SAVINGS', 'CHECKING'))
