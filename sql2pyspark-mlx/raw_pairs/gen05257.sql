SELECT 'payment' AS source, status FROM payments UNION ALL SELECT 'subscription' AS source, status FROM subscriptions ORDER BY status DESC
