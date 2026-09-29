SELECT * FROM (SELECT status, customer_id, account_type, balance FROM accounts) src PIVOT (SUM(balance) AS value_sum, COUNT(1) AS n FOR account_type IN ('LOAN', 'CREDIT'))
