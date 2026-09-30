SELECT status, 'order' AS kind FROM orders UNION ALL SELECT status, 'subscription' AS kind FROM subscriptions;
