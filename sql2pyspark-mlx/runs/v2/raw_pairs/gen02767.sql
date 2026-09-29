SELECT end_date FROM subscriptions WHERE status = 'CANCELLED' UNION SELECT end_date FROM subscriptions WHERE end_date BETWEEN '2023-12-01' AND '2025-09-01'
