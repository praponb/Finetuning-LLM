SELECT id, plan, end_date, start_date, FLOOR(monthly_fee) AS monthly_fee_whole FROM subscriptions WHERE status IN ('ACTIVE', 'EXPIRED', 'PAUSED')
