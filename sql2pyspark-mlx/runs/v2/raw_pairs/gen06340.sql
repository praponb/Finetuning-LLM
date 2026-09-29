SELECT status FROM orders INTERSECT SELECT status FROM subscriptions WHERE plan IN ('enterprise', 'standard');
