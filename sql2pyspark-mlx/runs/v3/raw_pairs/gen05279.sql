SELECT id, opened_date, DENSE_RANK() OVER (ORDER BY opened_date) AS opened_date_dense_rank FROM accounts;
