SELECT status FROM orders WHERE amount > 410 UNION SELECT status FROM subscriptions WHERE status IN ('CANCELLED', 'ACTIVE')
