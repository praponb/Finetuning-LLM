SELECT id, customer_id, start_date, NTILE(2) OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) AS start_date_ntile FROM subscriptions ORDER BY start_date_ntile;
