SELECT id, status, opened_date, balance, LEAD(balance, 1) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_balance FROM accounts;
