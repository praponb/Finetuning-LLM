SELECT id, customer_id, end_date, start_date FROM (SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY end_date) row_num FROM subscriptions WHERE customer_id IS NULL) t WHERE row_num <= 5
