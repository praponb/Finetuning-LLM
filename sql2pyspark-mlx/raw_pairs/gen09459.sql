SELECT id, customer_id, start_date, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY start_date) start_date_dense_rank FROM subscriptions ORDER BY customer_id ASC
