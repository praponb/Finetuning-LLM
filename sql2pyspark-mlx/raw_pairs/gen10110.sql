SELECT 'payment' AS kind, status FROM payments UNION ALL SELECT 'subscription' AS kind, status FROM subscriptions;
