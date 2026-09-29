SELECT id, status, opened_date, LEAD(opened_date, 1) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_opened_date FROM accounts WHERE balance >= 73000
