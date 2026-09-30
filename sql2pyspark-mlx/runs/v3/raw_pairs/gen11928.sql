SELECT id, start_date, status, end_date FROM subscriptions WHERE customer_id IS NOT NULL AND start_date > '2023-09-01' ORDER BY status DESC
