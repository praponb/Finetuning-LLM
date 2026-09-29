SELECT id, customer_id, status, end_date, NTILE(2) OVER (PARTITION BY customer_id, status ORDER BY end_date, id) AS end_date_ntile FROM subscriptions WHERE plan IN ('standard', 'premium', 'basic');
