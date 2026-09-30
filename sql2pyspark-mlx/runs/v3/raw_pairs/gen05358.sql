SELECT status FROM accounts WHERE customer_id IN (7, 13, 30, 35) UNION ALL SELECT status FROM subscriptions WHERE monthly_fee < 220 ORDER BY status
