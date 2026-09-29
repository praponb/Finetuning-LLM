SELECT opened_date, status, account_type FROM accounts WHERE balance BETWEEN 63000 AND 69000 AND (customer_id = 25 OR customer_id IN (2, 27, 32, 33))
