SELECT * FROM (SELECT status, account_type, balance FROM accounts)
PIVOT (SUM(balance) FOR account_type IN ('SAVINGS', 'CHECKING', 'CREDIT', 'LOAN'));
