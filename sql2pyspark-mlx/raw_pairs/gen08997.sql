SELECT end_date FROM subscriptions UNION ALL SELECT opened_date FROM accounts WHERE status NOT IN ('OPEN', 'CLOSED') ORDER BY end_date
