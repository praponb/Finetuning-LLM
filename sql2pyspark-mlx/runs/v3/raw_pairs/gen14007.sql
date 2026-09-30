SELECT id, account_type, opened_date, DENSE_RANK() OVER (PARTITION BY account_type ORDER BY opened_date) AS opened_date_dense_rank FROM accounts;
