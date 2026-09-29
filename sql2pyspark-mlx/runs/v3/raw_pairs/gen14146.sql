SELECT account_type, customer_id, balance, FLOOR(balance) AS balance_whole FROM accounts WHERE balance <= 57000 AND customer_id IS NOT NULL ORDER BY customer_id, balance_whole;
