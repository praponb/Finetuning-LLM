SELECT id, start_date FROM subscriptions WHERE plan = 'enterprise' UNION ALL SELECT id, start_date FROM subscriptions WHERE end_date BETWEEN '2023-12-01' AND '2024-06-01';
