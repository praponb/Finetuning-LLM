SELECT balance, customer_id, FLOOR(balance) AS balance_whole FROM accounts WHERE balance < 52000 ORDER BY customer_id DESC
