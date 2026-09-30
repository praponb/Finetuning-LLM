SELECT id, balance, opened_date, COALESCE(balance, 0) AS balance_filled, COALESCE(balance, 0) AS balance_filled_2 FROM accounts WHERE opened_date < '2023-09-30'
