SELECT id, status, opened_date, LAG(opened_date, 1) OVER (PARTITION BY status ORDER BY opened_date, id) prev_opened_date FROM accounts;
