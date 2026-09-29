SELECT start_date FROM subscriptions WHERE plan IN ('standard', 'premium') UNION SELECT start_date FROM subscriptions WHERE end_date >= '2024-01-01'
