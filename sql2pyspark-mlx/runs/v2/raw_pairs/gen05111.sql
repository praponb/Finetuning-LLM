SELECT id, start_date, monthly_fee, LEAD(monthly_fee, 2) OVER (ORDER BY start_date, id) AS next_monthly_fee FROM subscriptions WHERE status IN ('CANCELLED', 'PAUSED');
