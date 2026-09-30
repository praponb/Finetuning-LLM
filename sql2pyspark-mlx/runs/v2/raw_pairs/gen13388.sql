SELECT status FROM shipments UNION SELECT status FROM subscriptions WHERE status IN ('ACTIVE', 'PAUSED')
