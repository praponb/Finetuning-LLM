SELECT id, start_date, NTILE(10) OVER (ORDER BY start_date, id) AS start_date_ntile
FROM subscriptions;
