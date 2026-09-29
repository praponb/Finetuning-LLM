SELECT id, account_type, opened_date, RANK() OVER (PARTITION BY account_type ORDER BY opened_date) AS opened_date_rank FROM accounts WHERE customer_id = 17;
