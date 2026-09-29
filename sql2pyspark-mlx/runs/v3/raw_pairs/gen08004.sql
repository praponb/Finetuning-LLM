SELECT id, customer_id, opened_date, PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS opened_date_pct_rank FROM accounts ORDER BY id;
