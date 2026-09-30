SELECT id, plan FROM subscriptions WHERE customer_id = 12 AND (end_date < '2025-10-09' OR end_date > '2025-10-01')
