SELECT start_date FROM subscriptions UNION SELECT opened_date FROM accounts WHERE status NOT IN ('FROZEN', 'OPEN') ORDER BY start_date
