SELECT status FROM accounts EXCEPT SELECT status FROM subscriptions WHERE plan = 'enterprise' ORDER BY status DESC;
