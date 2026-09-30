SELECT status, 'payment' source FROM payments WHERE order_id IN (11, 15, 32, 39) UNION ALL SELECT status, 'subscription' source FROM subscriptions ORDER BY status;
