SELECT id, plan FROM subscriptions WHERE plan NOT IN ('standard', 'enterprise') UNION ALL SELECT id, plan FROM subscriptions WHERE plan = 'premium';
