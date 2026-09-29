SELECT status FROM shipments UNION ALL SELECT status FROM subscriptions WHERE MONTH(start_date) IN (5, 7, 10) ORDER BY status DESC
