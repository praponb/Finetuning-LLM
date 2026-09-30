SELECT id, opened_date, LAG(opened_date, 1) OVER (ORDER BY opened_date, id) AS prev_opened_date
FROM accounts;
