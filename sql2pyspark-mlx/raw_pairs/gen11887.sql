SELECT id, account_type, opened_date, balance, LAG(balance, 2) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS prev_balance FROM accounts ORDER BY balance ASC, opened_date ASC
