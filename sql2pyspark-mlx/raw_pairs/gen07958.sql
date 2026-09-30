SELECT id, opened_date, NTILE(2) OVER (ORDER BY opened_date, id) AS bucket FROM accounts
