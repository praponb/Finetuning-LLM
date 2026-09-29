SELECT id, start_date, NTILE(2) OVER (ORDER BY start_date, id) AS quartile
FROM subscriptions;
