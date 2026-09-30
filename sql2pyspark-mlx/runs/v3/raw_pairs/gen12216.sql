SELECT * FROM (SELECT id, status, opened_date, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC, id) AS pos FROM accounts) AS r WHERE pos <= 3 ORDER BY opened_date ASC
