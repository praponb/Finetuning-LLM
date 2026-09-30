SELECT * FROM (SELECT status, account_type, balance FROM accounts WHERE account_type <> 'CREDIT') PIVOT (COUNT(balance) FOR account_type IN ('SAVINGS', 'CHECKING'));
