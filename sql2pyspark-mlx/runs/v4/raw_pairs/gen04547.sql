SELECT id, plan, start_date, NTILE(2) OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS start_date_ntile FROM subscriptions WHERE customer_id IS NOT NULL
