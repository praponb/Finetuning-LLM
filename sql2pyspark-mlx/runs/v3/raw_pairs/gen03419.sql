SELECT id, start_date FROM subscriptions WHERE plan IN ('enterprise', 'standard') UNION SELECT id, start_date FROM subscriptions WHERE start_date > '2024-01-26' ORDER BY id
