SELECT id, status, start_date, DENSE_RANK() OVER (PARTITION BY status ORDER BY start_date DESC) AS dense_rnk FROM subscriptions;
