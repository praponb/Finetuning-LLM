SELECT customer_id, status, account_type, COALESCE(balance, 0) AS balance_filled FROM accounts WHERE opened_date > '2023-07-01' ORDER BY balance_filled, status;
