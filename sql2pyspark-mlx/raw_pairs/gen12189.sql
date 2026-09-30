SELECT * FROM (SELECT customer_id, account_type, balance FROM accounts WHERE status = 'FROZEN') PIVOT (SUM(balance) FOR account_type IN ('CREDIT', 'CHECKING', 'LOAN', 'SAVINGS'))
