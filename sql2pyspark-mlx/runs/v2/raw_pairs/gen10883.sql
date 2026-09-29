SELECT id, status, end_date, RANK() OVER (PARTITION BY status ORDER BY end_date) AS rnk FROM subscriptions;
