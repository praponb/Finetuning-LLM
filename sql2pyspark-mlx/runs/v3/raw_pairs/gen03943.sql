SELECT id, end_date, start_date FROM subscriptions WHERE start_date > '2024-01-01' UNION ALL SELECT id, end_date, start_date FROM subscriptions WHERE end_date > '2023-06-30';
