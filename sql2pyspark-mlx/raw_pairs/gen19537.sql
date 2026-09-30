SELECT id, customer_id, start_date, RANK() OVER (PARTITION BY customer_id ORDER BY start_date) rnk FROM subscriptions WHERE status = 'CANCELLED';
