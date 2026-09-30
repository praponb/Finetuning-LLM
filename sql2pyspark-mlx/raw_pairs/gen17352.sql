SELECT id, plan FROM subscriptions WHERE status = 'PAUSED' UNION SELECT id, plan FROM subscriptions WHERE plan = 'enterprise';
