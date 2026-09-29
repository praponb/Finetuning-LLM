SELECT id, status, opened_date, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) row_num FROM accounts
