SELECT id, opened_date, ROW_NUMBER() OVER (ORDER BY opened_date, id) AS row_num
FROM accounts;
