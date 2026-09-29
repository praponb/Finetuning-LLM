SELECT status, 'payment' AS source FROM payments WHERE order_id IS NULL UNION SELECT status, 'subscription' AS source FROM subscriptions WHERE status = 'CANCELLED' ORDER BY source;
