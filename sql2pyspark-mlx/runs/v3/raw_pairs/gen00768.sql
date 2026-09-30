SELECT end_date FROM subscriptions WHERE status NOT IN ('PAUSED', 'ACTIVE', 'EXPIRED') UNION ALL SELECT signup_date AS end_date FROM customers
