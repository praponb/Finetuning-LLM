SELECT id, balance, opened_date, customer_id FROM accounts WHERE balance >= (SELECT MAX(balance) FROM accounts)
