SELECT status, 'payment' AS source FROM payments UNION SELECT status, 'subscription' AS source FROM subscriptions ORDER BY source DESC;
