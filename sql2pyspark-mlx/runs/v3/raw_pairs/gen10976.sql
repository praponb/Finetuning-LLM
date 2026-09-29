SELECT * FROM (SELECT status, account_type, balance FROM accounts WHERE opened_date > '2023-05-01') PIVOT (AVG(balance) FOR account_type IN ('CREDIT', 'SAVINGS'));
