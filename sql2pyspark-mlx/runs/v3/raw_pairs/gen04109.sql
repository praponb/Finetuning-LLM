SELECT id, account_type, status, opened_date, LAG(opened_date, 1) OVER (PARTITION BY account_type, status ORDER BY opened_date, id) AS prev_opened_date FROM accounts
