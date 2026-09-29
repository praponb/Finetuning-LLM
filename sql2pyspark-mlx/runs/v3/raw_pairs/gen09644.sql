SELECT status FROM shipments INTERSECT SELECT status FROM subscriptions WHERE MONTH(start_date) IN (3, 6, 12) ORDER BY status
