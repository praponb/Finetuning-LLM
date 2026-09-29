SELECT status FROM shipments UNION SELECT status FROM subscriptions WHERE YEAR(start_date) >= 2025 ORDER BY status;
