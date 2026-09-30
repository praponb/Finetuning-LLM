SELECT id, status, end_date, DENSE_RANK() OVER (PARTITION BY status ORDER BY end_date) AS end_date_dense_rank FROM subscriptions;
