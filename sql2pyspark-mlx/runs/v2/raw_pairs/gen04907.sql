SELECT id, customer_id, opened_date, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS opened_date_dense_rank FROM accounts ORDER BY opened_date_dense_rank, id;
