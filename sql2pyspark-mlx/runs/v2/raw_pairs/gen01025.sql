SELECT id, status, start_date, RANK() OVER (PARTITION BY status ORDER BY start_date) AS rnk FROM subscriptions
