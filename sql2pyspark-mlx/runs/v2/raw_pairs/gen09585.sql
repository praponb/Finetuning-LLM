SELECT status FROM accounts WHERE opened_date > '2025-08-01' UNION ALL SELECT status FROM subscriptions WHERE status = 'CANCELLED' ORDER BY status
