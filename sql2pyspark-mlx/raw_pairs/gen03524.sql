SELECT 'order' AS kind, status FROM orders WHERE shipping_fee BETWEEN 30 AND 34 UNION SELECT 'subscription' AS kind, status FROM subscriptions ORDER BY status;
