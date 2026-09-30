SELECT * FROM (SELECT status, account_type, balance FROM accounts)
PIVOT (MAX(balance) FOR account_type IN ('CHECKING', 'CREDIT', 'SAVINGS'));
