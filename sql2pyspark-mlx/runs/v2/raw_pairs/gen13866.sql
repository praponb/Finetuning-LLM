SELECT status FROM accounts WHERE status IN ('OPEN', 'FROZEN', 'CLOSED') UNION SELECT status FROM subscriptions WHERE plan IN ('enterprise', 'basic', 'standard') ORDER BY status DESC;
