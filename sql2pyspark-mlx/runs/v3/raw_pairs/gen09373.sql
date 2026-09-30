SELECT delivered_date FROM shipments UNION ALL SELECT start_date FROM subscriptions WHERE YEAR(end_date) = 2025
