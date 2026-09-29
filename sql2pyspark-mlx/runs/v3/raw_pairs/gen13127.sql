SELECT id, end_date, status FROM subscriptions WHERE end_date = (SELECT MAX(end_date) FROM subscriptions WHERE status IN ('EXPIRED', 'ACTIVE', 'CANCELLED'))
