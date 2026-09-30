SELECT id, plan, start_date, status FROM subscriptions WHERE start_date > '2023-07-23' OR status IN ('PAUSED', 'EXPIRED', 'ACTIVE') ORDER BY plan DESC;
