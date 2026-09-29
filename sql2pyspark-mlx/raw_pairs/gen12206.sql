SELECT 'payment' AS kind, status FROM payments WHERE order_id IS NULL UNION SELECT 'subscription' AS kind, status FROM subscriptions;
