SELECT status FROM accounts INTERSECT SELECT status FROM subscriptions WHERE plan = 'premium' ORDER BY status;
