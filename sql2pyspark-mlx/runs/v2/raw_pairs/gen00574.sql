SELECT id, status, start_date, NTILE(4) OVER (PARTITION BY status ORDER BY start_date, id) AS start_date_ntile FROM subscriptions WHERE status = 'ACTIVE'
