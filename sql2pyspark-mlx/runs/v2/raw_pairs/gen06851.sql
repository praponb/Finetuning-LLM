SELECT end_date, start_date FROM subscriptions WHERE YEAR(start_date) = 2024 AND start_date > '2024-11-01' AND status NOT IN ('EXPIRED', 'PAUSED', 'CANCELLED');
